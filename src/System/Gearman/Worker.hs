{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module System.Gearman.Worker
(
    Job,
    WorkerFunc,
    JobError,
    addFunc,
    Worker,
    runWorker,
    work
) where

import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.Chan
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
    jobData :: [S.ByteString],
    sendWarning :: (S.ByteString -> IO ()),
    sendData    :: (S.ByteString -> IO ()),
    sendStatus  :: (JobStatus -> IO ())
}

-- |The data passed to a worker when a job is received.
data JobSpec = JobSpec {
    rawJobData :: S.ByteString,
    jobName    :: S.ByteString,
    jobFunc    :: WorkerFunc,
    outChan    :: Chan S.ByteString,
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
    workerMsgChan :: TChan S.ByteString,
    workerSemaphore :: TBChan Bool
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
    sem <- (liftIO . atomically . newTBChan) nWorkers
    evalStateT action $ Work M.empty nWorkers outChan sem

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
    put $ Work (M.insert ident cap funcMap) nWorkers workerMsgChan workerSemaphore
    liftGearman $ sendPacket packet

-- |startWork handles communication with the server, dispatching of 
-- worker threads and reporting of results.
work :: Worker (Maybe GearmanError)
work = do
    Work{..} <- get
    return Nothing

-- |Run a worker with a job. 
doWork :: JobSpec -> IO ()
doWork JobSpec{..} = do
    waitForWorkerSlot semaphore
    let args = unpackData rawJobData
    let job = Job (unpackData rawJobData)
                  (dataCallback jobHandle outChan)
                  (warningCallback jobHandle outChan)
                  (statusCallback jobHandle outChan)
    res <- jobFunc job
    case res of
        Left err -> writeChan outChan $ buildErrorPacket jobHandle err
        Right payload -> writeChan outChan $ buildWorkCompleteReq jobHandle payload
    openWorkerSlot semaphore
  where
    dataCallback h c payload    = writeChan c $ buildWorkDataReq h payload
    warningCallback h c payload = writeChan c $ buildWorkWarningReq h payload
    statusCallback h c status   = writeChan c $ buildWorkStatusReq h status
    buildErrorPacket handle err = case err of
        Nothing -> buildWorkFailReq handle
        Just msg -> buildWorkExceptionReq handle msg
    waitForWorkerSlot          = void . atomically . readTBChan 
    openWorkerSlot             = atomically . flip writeTBChan  True
