{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module System.Gearman.Worker
(
    Job,
    WorkerFunc,
    JobError,
    addFunc,
    newCapability
) where

import qualified Data.ByteString.Lazy as S
import Data.Either
import Control.Monad
import Control.Applicative
import Control.Monad.State.Strict
import qualified Data.Map as M
import Data.Map (Map,empty)
import Control.Concurrent.Async
import Control.Concurrent.Chan
import Control.Concurrent.STM.TBChan
import Control.Concurrent.STM

import System.Gearman.Error
import System.Gearman.Connection
import System.Gearman.Protocol

data WorkerFunc = WorkerFunc (Job -> IO (Either JobError S.ByteString))

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
    return $ Capability ident (WorkerFunc f) timeout 

type WorkerWarning = S.ByteString

type WorkerData = S.ByteString

type WorkerStatus = (Int, Int)

data WorkerMessage = WorkerWarning | WorkerData | WorkerStatus

-- |The data passed to a worker function when running a job.
data Job = Job {
    jobData :: [S.ByteString],
    sendWarning :: (S.ByteString -> IO ()),
    sendData    :: (S.ByteString -> IO ()),
    sendStatus  :: (S.ByteString -> IO ())
}

-- |The data passed to a worker when a job is received.
data JobSpec = JobSpec {
    rawJobData :: S.ByteString,
    jobName    :: S.ByteString,
    jobFunc    :: WorkerFunc,
    outChan    :: Chan WorkerMessage
}
    

data JobError = JobError {
    error :: !S.ByteString
}

-- |Maintained by the controller, defines the mapping between 
-- function identifiers read from the server and Haskell functions.
type WorkMap = Map S.ByteString Capability

-- |Internal state for the Worker monad.
data Work = Work {
    workMap :: WorkMap,
    nWorkers :: Int
}

-- |This monad maintains a worker's state, and must be run within
-- the Gearman monad.
newtype Worker a = Worker (StateT Work Gearman a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadState Work)

liftGearman :: Gearman a -> Worker a
liftGearman = Worker . lift

runWorker :: Int -> Worker a -> Gearman a
runWorker nWorkers (Worker action) = 
    evalStateT action $ Work M.empty nWorkers

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
    put $ Work (M.insert ident cap workMap) nWorkers
    liftGearman $ sendPacket packet

-- |The controller handles communication with the server, dispatching of 
-- worker threads and reporting of results.
controller :: Worker (Maybe GearmanError)
controller = do
    Work{..} <- get
    case (M.null workMap) of
        True -> return Nothing -- nothing to do, so exit without error
        False -> undefined

doWork :: Chan JobSpec -> IO ()
doWork jobChan = forever $ do
    JobSpec{..} <- readChan jobChan
    undefined
    
