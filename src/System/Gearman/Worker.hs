-- This file is part of gearman-haskell.
--
-- Copyright 2014 Anchor Systems Pty Ltd and others.
--
-- The code in this file, and the program it is a part of, is made
-- available to you by its authors as open source software: you can
-- redistribute it and/or modify it under the terms of the BSD license.

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
    runWorkAsync,
    work
) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Concurrent.STM.TBChan
import Control.Monad
import Control.Monad.Reader
import Control.Monad.State.Strict
import qualified Data.ByteString.Lazy as L
import Data.Map (Map)
import qualified Data.Map as M

import System.Gearman.Error
import System.Gearman.Connection
import System.Gearman.Protocol
import System.Gearman.Job

-- |A WorkerFunc is a callback defined by the worker, taking a Job and
-- returning either an error or some data.
type WorkerFunc = Job -> IO (Either JobError L.ByteString)

-- |A Capability is something a worker can do.
data Capability = Capability {
    ident   :: L.ByteString,
    func    :: WorkerFunc,
    timeout :: Maybe Int
}

-- |Initialize a new Capability from initial job data.
newCapability :: L.ByteString ->
                 WorkerFunc ->
                 Maybe Int ->
                 Worker Capability
newCapability ident f timeout = do
--    outChan <- liftIO $ atomically $ newTBChan 1
    return $ Capability ident f timeout

-- |The data passed to a worker function when running a job.
data Job = Job {
    jobData     :: L.ByteString,
    sendWarning :: (L.ByteString -> IO ()),
    sendData    :: (L.ByteString -> IO ()),
    sendStatus  :: (JobStatus -> IO ())
}

-- |The data passed to a worker when a job is received.
data JobSpec = JobSpec {
    jobArg      :: L.ByteString,
    jobName     :: L.ByteString,
    jobFunc     :: WorkerFunc,
    outChan     :: TChan L.ByteString,
    semaphore   :: TBChan Bool,
    jobHandle   :: L.ByteString,
    clientId    :: Maybe L.ByteString
}

-- Error returned by a job. A Nothing will be sent as a WORK_FAIL
-- packet; Just err will be sent as a WORK_EXCEPTION packet with err
-- as the data argument.
type JobError = Maybe L.ByteString

-- |Maintained by the controller, defines the mapping between
-- function identifiers read from the server and Haskell functions.
type FuncMap = Map L.ByteString Capability

-- |Whether the worker is dormant because there's nothing for it to do
-- or not.
data WorkerState = WorkerConnected | WorkerSleeping
    deriving (Show, Eq)

-- |Internal state for the Worker monad.
data Work = Work {
    funcMap         :: FuncMap,
    nWorkers        :: Int,
    -- FIXME: encapsulate this in a packet type
    outgoingChan    :: TChan L.ByteString,
    incomingChan    :: TChan GearmanPacket,
    workerSemaphore :: TBChan Bool,
    workerJobChan   :: TChan JobSpec,
    processState    :: MVar WorkerState
}

-- |This monad maintains a worker's state, and must be run within
-- the Gearman monad.
newtype Worker a = Worker (StateT Work Gearman a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadState Work)

liftGearman :: Gearman a -> Worker a
liftGearman = Worker . lift

-- Runs an action in the Worker monad using the specified number of
-- worker threads to execute jobs.
runWorker :: Int -> Worker a -> Gearman a
runWorker nWorkers (Worker action) = do
    outChan <- (liftIO . atomically) newTChan
    inChan  <- (liftIO . atomically) newTChan
    jobChan <- (liftIO . atomically) newTChan
    sem     <- (liftIO . atomically . newTBChan) nWorkers
    replicateM_ nWorkers $ seed sem
    stateVar <- liftIO $ newMVar WorkerConnected
    evalStateT action $ Work M.empty nWorkers outChan inChan sem jobChan stateVar
  where
    seed = liftIO . atomically . flip writeTBChan True

newtype WorkAsync a = WorkAsync (StateT Work Gearman a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadState Work)

runWorkAsync :: Worker a -> Worker ()
runWorkAsync (Worker action) = do
    w <- get
    liftGearman $ runGearmanAsync $ evalStateT action w

-- |addFunc registers a function with the server as performable by a
-- worker.
addFunc :: L.ByteString ->
           WorkerFunc ->
           Maybe Int ->
           Worker (Maybe GearmanError)
addFunc name f tout = do
    Work{..} <- get
    cap@Capability{..} <- newCapability name f tout
    let packet = case timeout of
                    Nothing -> buildCanDoReq ident
                    Just t  -> buildCanDoTimeoutReq ident t
    put $ Work (M.insert ident cap funcMap)
               nWorkers
               outgoingChan
               incomingChan
               workerSemaphore
               workerJobChan
               processState
    liftGearman $ sendPacket packet

-- |Receiver handles incoming packets.
receiver :: Worker ()
receiver = forever $ do
    incoming <- liftGearman . recvPacket $ DomainWorker
    case incoming of
        Left err -> liftIO $ putStrLn (show err)
        Right pkt -> routeIncoming pkt


-- |Given an ASSIGN_JOB or ASSIGN_JOB_UNIQ packet, give the job to a worker
-- thread.
assignJobUniq :: GearmanPacket -> Worker ()
assignJobUniq pkt = do
    Work{..} <- get
    let GearmanPacket{..} = pkt
    let PacketHeader{..} = header
    let spec = case (parseSpecs args) of
         Nothing -> Left "error: not enough arguments provided to JOB_ASSIGN_UNIQ"
         Just (handle, fnId, clientId, dataArg) -> case (M.lookup fnId funcMap) of
             Nothing -> Left $  "error: no function with name " ++ (show fnId)
             Just Capability{..}  ->
                Right $ JobSpec dataArg
                                fnId
                                func
                                outgoingChan
                                workerSemaphore
                                handle
                                (Just clientId)
    case spec of
        Left err -> liftIO $ putStrLn err
        Right js -> do
            liftIO $ atomically $ writeTChan workerJobChan js
  where
    parseSpecs (handle:fnId:clientId:dataArg:_) = Just (handle, fnId, clientId, dataArg)
    parseSpecs _                                = Nothing

-- |Given an ASSIGN_JOB or ASSIGN_JOB_UNIQ packet, give the job to a worker
-- thread.
assignJob :: GearmanPacket -> Worker ()
assignJob pkt = do
    Work{..} <- get
    let GearmanPacket{..} = pkt
    let PacketHeader{..} = header
    let spec = case (parseSpecs args) of
         Nothing -> Left "error: not enough arguments provided to JOB_ASSIGN"
         Just (handle, fnId, dataArg) -> case (M.lookup fnId funcMap) of
             Nothing -> Left $  "error: no function with name " ++ (show fnId)
             Just Capability{..}  ->
                 Right $ JobSpec dataArg
                                 fnId
                                 func
                                 outgoingChan
                                 workerSemaphore
                                 handle
                                 Nothing
    case spec of
        Left err -> liftIO $ putStrLn err
        Right js -> do
            liftIO $ atomically $ writeTChan workerJobChan js
  where
    parseSpecs (handle:fnId:dataArg:_) = Just (handle, fnId, dataArg)
    parseSpecs _                       = Nothing

-- |Handle an incoming packet from the Gearman server.
routeIncoming :: GearmanPacket -> Worker ()
routeIncoming pkt = do
    Work{..} <- get
    let GearmanPacket{..} = pkt
    let PacketHeader{..} = header
    case packetType of
        JobAssign -> assignJob pkt
        JobAssignUniq -> assignJobUniq pkt
        NoJob         -> do
            void $ liftIO $ swapMVar processState WorkerSleeping
            liftIO $ writeSleep outgoingChan -- We will sleep until the server has jobs for us
            return () -- Server has no work for us, we do nothing; may want to sleep here instead.
        Noop          -> void $ liftIO $ swapMVar processState WorkerConnected
        typ           -> liftIO $ putStrLn $ "Unexpected packet of type " ++ (show typ)
  where
    writeSleep      = atomically . flip writeTChan buildPreSleepReq

-- |startWork handles communication with the server, dispatching of
-- worker threads and reporting of results.
work :: Worker ()
work = do
    Work{..} <- get
    runWorkAsync dispatchWorkers
    runWorkAsync receiver
    forever $ do
        gotOut <- (liftIO . noMessages) outgoingChan >>= (return . not)
        case gotOut of
            False -> return ()
            True  -> readMessage outgoingChan >>= sendMessage
        liftIO $ threadDelay 10000
  where
    noMessages  = liftIO . atomically . isEmptyTChan
    readMessage = liftIO . atomically . readTChan
    sendMessage msg = do
        res <- liftGearman $ sendPacket msg
        case res of
            Nothing  -> return ()
            Just err -> liftIO $ printError err

waitForWorkerSlot :: TBChan Bool -> IO ()
waitForWorkerSlot = void . atomically . readTBChan

openWorkerSlot :: TBChan Bool -> IO ()
openWorkerSlot = atomically . flip writeTBChan  True

dispatchWorkers :: Worker ()
dispatchWorkers = forever $ do
    Work{..} <- get
    st <- liftIO $ readMVar processState
    case st of
        WorkerConnected -> liftIO $ do
 --           liftIO $ putStrLn "dispatchWorkers thinks worker is connected"
            waitForWorkerSlot workerSemaphore
            writeJobRequest outgoingChan
            openWorkerSlot workerSemaphore
            noJobs <- (atomically . isEmptyTChan) workerJobChan
            if (not noJobs) then do
                spec <- readJob workerJobChan
                async (doWork spec) >>= link
            else liftIO $ threadDelay 1000
        WorkerSleeping -> liftIO $ do
            threadDelay 1000000
            return ()
  where
    writeJobRequest = atomically . flip writeTChan buildGrabJobReq
    readJob         = atomically . readTChan

-- |Run a worker with a job. This will block until there are fewer than
-- nWorkers running, and terminate when complete.
doWork :: JobSpec -> IO ()
doWork JobSpec{..} = do
    waitForWorkerSlot semaphore
    let job = Job jobArg
                  (dataCallback    jobHandle outChan)
                  (warningCallback jobHandle outChan)
                  (statusCallback  jobHandle outChan)
    res <- jobFunc job
    case res of
        Left err      -> (liftIO . atomically . writeTChan outChan) $ buildErrorPacket jobHandle err
        Right payload -> do
            (liftIO . atomically . writeTChan outChan) $ buildWorkCompleteReq jobHandle payload
    openWorkerSlot semaphore
  where
    dataCallback h c payload    = (atomically . writeTChan c) $ buildWorkDataReq    h payload
    warningCallback h c payload = (atomically . writeTChan c) $ buildWorkWarningReq h payload
    statusCallback h c status   = (atomically . writeTChan c) $ buildWorkStatusReq  h status
    buildErrorPacket handle err = case err of
        Nothing -> buildWorkFailReq handle
        Just msg -> buildWorkExceptionReq handle msg
