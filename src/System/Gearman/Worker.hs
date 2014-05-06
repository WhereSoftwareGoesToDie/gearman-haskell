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

addFunc :: S.ByteString -> WorkerFunc -> Int -> IO (Maybe GearmanError)
addFunc fnId f timeout = undefined
