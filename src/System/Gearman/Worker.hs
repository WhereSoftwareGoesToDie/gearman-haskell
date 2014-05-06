{-# LANGUAGE RecordWildCards #-}

module System.Gearman.Worker
(
    Job,
    WorkerFunc
) where

import qualified Data.ByteString.Lazy as S
import Data.Either
import Control.Monad

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

addFunc :: Connection -> S.ByteString -> WorkerFunc -> Maybe Int -> IO (Maybe GearmanError)
addFunc Connection{..} fnId f timeout = do
    packet <- case timeout of 
        Nothing -> buildCanDoReq fnId
        Just t  -> buildCanDoTimeoutReq fnId t
    sendPacket packet
