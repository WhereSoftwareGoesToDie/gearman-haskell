{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module System.Gearman.Worker
(
    Job(..),
    WorkerFunc,
    JobError,
    addFunc,
    Worker,
    runWorker,
    work
) where

import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Concurrent.STM.TBChan
import Control.Monad
import Control.Monad.State.Strict
import qualified Data.ByteString.Lazy as S
import Data.Either
import Data.Map (Map,empty)
import qualified Data.Map as M

import System.Gearman.Error
import System.Gearman.Connection
import System.Gearman.Protocol
import System.Gearman.Job

type WorkerFunc = (Job -> IO (Either JobError S.ByteString))

-- |A Capability is something a worker can do.
data Capability = Capability {
    ident :: S.ByteString,
    func :: WorkerFunc,
    timeout :: Maybe Int
}

-- |Initialize a new Capability from initial job data.
newCapability :: S.ByteString -> 
              (Job -> IO (Either JobError S.ByteString)) -> 
              Maybe Int ->
              Worker Capability
newCapability ident f timeout = do
--    outChan <- liftIO $ atomically $ newTBChan 1
    return $ Capability ident f timeout 

-- |The data passed to a worker function when running a job.
data Job = Job {
    jobData     :: S.ByteString,
    sendWarning :: (S.ByteString -> IO ()),
    sendData    :: (S.ByteString -> IO ()),
    sendStatus  :: (JobStatus -> IO ())
}

-- |The data passed to a worker when a job is received.
data JobSpec = JobSpec {
    jobArg :: S.ByteString,
    jobName    :: S.ByteString,
    jobFunc    :: WorkerFunc,
    outChan    :: TChan S.ByteString,
    semaphore  :: TBChan Bool,
    jobHandle  :: S.ByteString
}

type JobError = Maybe S.ByteString

-- |Maintained by the controller, defines the mapping between 
-- function identifiers read from the server and Haskell functions.
type FuncMap = Map S.ByteString Capability

-- |Internal state for the Worker monad.
data Work = Work {
    funcMap :: FuncMap,
    nWorkers :: Int,
    -- FIXME: encapsulate this in a packet type
    outgoingChan :: TChan S.ByteString,
    incomingChan :: TChan GearmanPacket,
    workerSemaphore :: TBChan Bool,
    workerJobChan :: TChan JobSpec
}

-- |This monad maintains a worker's state, and must be run within
-- the Gearman monad.
newtype Worker a = Worker (StateT Work Gearman a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadState Work)

liftGearman :: Gearman a -> Worker a
liftGearman = Worker . lift

runWorker :: Int -> Worker a -> Gearman a
runWorker nWorkers (Worker action) = do
    outChan <- (liftIO . atomically) newTChan
    inChan <- (liftIO . atomically) newTChan
    jobChan <- (liftIO . atomically) newTChan
    sem <- (liftIO . atomically . newTBChan) nWorkers
    liftIO $ putStrLn $ "Seeding semaphore for " ++ (show nWorkers) ++ " workers"
    replicateM_ nWorkers $ seed sem
    evalStateT action $ Work M.empty nWorkers outChan inChan sem jobChan
  where
    seed = liftIO . atomically . flip writeTBChan True

-- |addFunc registers a function with the server as performable by a 
-- worker.
addFunc :: S.ByteString ->
           (Job -> IO (Either JobError S.ByteString)) -> 
           Maybe Int ->
           Worker (Maybe GearmanError)
addFunc name f tout = do
    Work{..} <- get
    cap <- newCapability name f tout
    let Capability{..} = cap
    let packet = case timeout of
                    Nothing -> buildCanDoReq ident
                    Just t  -> buildCanDoTimeoutReq ident t
    put $ Work (M.insert ident cap funcMap) nWorkers outgoingChan incomingChan workerSemaphore workerJobChan
    liftGearman $ sendPacket packet

linkWorkerThread :: Worker a -> IO ()
linkWorkerThread action = async (return action) >>= link

receiver :: Worker ()
receiver = forever $ do
    Work{..} <- get
    incoming <- liftGearman $ recvPacket DomainWorker
    case incoming of 
        Left err -> liftIO $ putStrLn (show err)
        Right pkt -> liftIO $ atomically $ writeTChan incomingChan pkt
    return ()

assignJob :: GearmanPacket -> Worker ()
assignJob pkt = do
    Work{..} <- get
    let GearmanPacket{..} = pkt
    let PacketHeader{..} = header
    let spec = case (parseSpecs args) of 
         Nothing                      -> Left "error: not enough arguments provided to JOB_ASSIGN"
         Just (handle, fnId, dataArg) -> case (M.lookup fnId funcMap) of
             Nothing -> Left $  "error: no function with name " ++ (show fnId)
             Just Capability{..}  -> Right $ JobSpec dataArg
                                                     fnId
                                                     func
                                                     outgoingChan                           
                                                     workerSemaphore
                                                     handle
    case spec of 
        Left err -> liftIO $ putStrLn err
        Right js -> liftIO $ atomically $ writeTChan workerJobChan js
  where
    parseSpecs args = case args of
        (handle:args') -> case args' of 
            (fnId:args'') -> case args'' of
                (dataArg:_)      -> Just (handle, fnId, dataArg)
                []               -> Nothing
            []           -> Nothing
        []            -> Nothing

routeIncoming :: GearmanPacket -> Worker ()
routeIncoming pkt = do
    let GearmanPacket{..} = pkt
    let PacketHeader{..} = header
    case packetType of
        JobAssign -> assignJob pkt
        JobAssignUniq -> assignJob pkt
        typ           -> liftIO $ putStrLn $ "Unexpected packet of type " ++ (show typ)

-- |startWork handles communication with the server, dispatching of 
-- worker threads and reporting of results.
work :: Worker ()
work = do
    Work{..} <- get
    liftIO $ putStrLn "dispatching workers"
    liftIO $ async (return dispatchWorkers) >>= link
    liftIO $ linkWorkerThread receiver
    forever $ do
        gotOut <- (liftIO . noMessages) outgoingChan >>= (return . not)
        case gotOut of 
            False -> return ()
            True  -> readMessage outgoingChan >>= sendMessage
        gotIn <- (liftIO . noMessages) incomingChan >>= (return . not)
        case gotIn of
            False -> return ()
            True  -> readMessage incomingChan >>= routeIncoming
  where
    noMessages = liftIO . atomically . isEmptyTChan
    readMessage  = liftIO . atomically . readTChan
    sendMessage msg = do
        res <- liftGearman $ sendPacket msg
        case res of 
            Nothing -> return ()
            Just err -> liftIO $ printError err

waitForWorkerSlot :: TBChan Bool -> IO ()
waitForWorkerSlot = void . atomically . readTBChan 

openWorkerSlot :: TBChan Bool -> IO ()
openWorkerSlot = atomically . flip writeTBChan  True

dispatchWorkers :: Worker ()
dispatchWorkers = forever $ do
    Work{..} <- get
    liftIO $ putStrLn "worker thread: waiting on semaphore"
    liftIO $ waitForWorkerSlot workerSemaphore
    liftIO $ putStrLn "worker thread: grabbing job"
    liftIO $ writeJobRequest outgoingChan
    spec <- liftIO $ readJob workerJobChan 
    liftIO $ openWorkerSlot workerSemaphore
    liftIO $ async (doWork spec) >>= link
    return ()
  where
    writeJobRequest = atomically . flip writeTChan buildGrabJobReq
    readJob = atomically . readTChan

-- |Run a worker with a job. This will block until there are fewer than 
-- nWorkers running, and terminate when complete.
doWork :: JobSpec -> IO ()
doWork JobSpec{..} = do
    waitForWorkerSlot semaphore
    let job = Job jobArg
                  (dataCallback jobHandle outChan)
                  (warningCallback jobHandle outChan)
                  (statusCallback jobHandle outChan)
    res <- jobFunc job
    case res of
        Left err -> (liftIO . atomically . writeTChan outChan) $ buildErrorPacket jobHandle err
        Right payload -> (liftIO . atomically . writeTChan outChan) $ buildWorkCompleteReq jobHandle payload
    openWorkerSlot semaphore
  where
    dataCallback h c payload    = (atomically . writeTChan c) $ buildWorkDataReq h payload
    warningCallback h c payload = (atomically . writeTChan c) $ buildWorkWarningReq h payload
    statusCallback h c status   = (atomically . writeTChan c) $ buildWorkStatusReq h status
    buildErrorPacket handle err = case err of
        Nothing -> buildWorkFailReq handle
        Just msg -> buildWorkExceptionReq handle msg
