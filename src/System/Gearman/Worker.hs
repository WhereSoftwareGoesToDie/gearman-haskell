{-# LANGUAGE RecordWildCards #-}

module System.Gearman.Worker
(
    Job,
    WorkerFunc,
    JobError,
    addFunc,
    workerFunc
) where

import qualified Data.ByteString.Lazy as S
import Data.Either
import Control.Monad
import qualified Control.Monad.Reader as MR
import qualified Control.Monad.Trans.State as STM
import Control.Monad.State.Class
import Control.Monad.State.Strict
import qualified Data.Map as M
import Data.Map (Map)

import System.Gearman.Error
import System.Gearman.Connection
import System.Gearman.Protocol

data Job = Job {
    jobData      :: [S.ByteString],
    fn           :: S.ByteString,
    sendWarning  :: (S.ByteString -> ()),
    sendData     :: (S.ByteString -> ()),
    updateStatus :: (Int -> Int -> ()),
    handle       :: S.ByteString,
    uniqId       :: S.ByteString
}

data JobError = JobError {
    error :: !S.ByteString
}

data WorkerFunc = WorkerFunc (Job -> IO (Either JobError S.ByteString))

data WorkMap = WorkMap (Map S.ByteString WorkerFunc)

data Work = Work {
    map :: WorkMap,
    conn :: Connection,
    nWorkers :: Int
}

newtype Worker a = Worker (StateT Work Gearman a)

runWorker :: Int -> Worker a -> Gearman a
runWorker nWorkers (Worker action) = do
    c <- MR.ask
    r <- runStateT action $ Work (WorkMap M.empty) c nWorkers
    return $ fst r

workerFunc :: (Job -> IO (Either JobError S.ByteString)) -> WorkerFunc
workerFunc f = WorkerFunc f

addFunc :: S.ByteString -> WorkerFunc -> Maybe Int -> Gearman (Maybe GearmanError)
addFunc fnId f timeout = do
    Connection{..} <- MR.ask
    packet <- case timeout of 
        Nothing -> return $ buildCanDoReq fnId
        Just t  -> return $ buildCanDoTimeoutReq fnId t
    sendPacket packet
