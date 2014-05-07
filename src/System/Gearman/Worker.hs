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
import Control.Monad.Reader
import Control.Monad.Trans.State

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

data Work = Work {
    map :: WorkMap
}

newtype Worker a = Worker (StateT Work Gearman a)

workerFunc :: (Job -> IO (Either JobError S.ByteString)) -> WorkerFunc
workerFunc f = WorkerFunc f

addFunc :: S.ByteString -> WorkerFunc -> Maybe Int -> Gearman (Maybe GearmanError)
addFunc fnId f timeout = do
    Connection{..} <- ask
    packet <- case timeout of 
        Nothing -> return $ buildCanDoReq fnId
        Just t  -> return $ buildCanDoTimeoutReq fnId t
    sendPacket packet
