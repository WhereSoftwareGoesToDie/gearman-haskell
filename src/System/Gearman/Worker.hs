{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

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
import Control.Applicative
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

type WorkMap = Map S.ByteString WorkerFunc

data Work = Work {
    workMap :: WorkMap,
    nWorkers :: Int
}

newtype Worker a = Worker (StateT Work Gearman a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadState Work)

liftGearman :: Gearman a -> Worker a
liftGearman = Worker . lift

runWorker :: Int -> Worker a -> Gearman a
runWorker nWorkers (Worker action) = 
    evalStateT action $ Work M.empty nWorkers

workerFunc :: (Job -> IO (Either JobError S.ByteString)) -> WorkerFunc
workerFunc = WorkerFunc

addFunc :: S.ByteString -> WorkerFunc -> Maybe Int -> Worker (Maybe GearmanError)
addFunc fnId f timeout = do
    Work{..} <- get
    let packet = case timeout of
                    Nothing -> buildCanDoReq fnId
                    Just t  -> buildCanDoTimeoutReq fnId t
    put $ Work (M.insert fnId f workMap) nWorkers
    liftGearman $ sendPacket packet
