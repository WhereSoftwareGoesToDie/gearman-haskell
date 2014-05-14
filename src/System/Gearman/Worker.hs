{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module System.Gearman.Worker
(
    Job,
    WorkerFunc,
    JobError,
    addFunc,
    getCapability
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

data Capability = Capability {
    ident :: S.ByteString,
    func :: WorkerFunc,
    timeout :: Maybe Int
}

getCapability :: S.ByteString -> 
              (Job -> IO (Either JobError S.ByteString)) -> 
              Maybe Int ->
              Capability 
getCapability ident f timeout = Capability ident (WorkerFunc f) timeout

type WorkMap = Map S.ByteString Capability

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

addFunc :: Capability -> Worker (Maybe GearmanError)
addFunc capability = do
    Work{..} <- get
    let Capability{..} = capability
    let packet = case timeout of
                    Nothing -> buildCanDoReq ident
                    Just t  -> buildCanDoTimeoutReq ident t
    put $ Work (M.insert ident capability workMap) nWorkers
    liftGearman $ sendPacket packet

controller :: Worker (Maybe GearmanError)
controller = do
    Work{..} <- get
    case (M.null workMap) of
        True -> return Nothing -- nothing to do, so exit without error
        False -> undefined

performJobs :: [Capability] -> Worker (Maybe GearmanError)
performJobs capabilities = do
    results <- mapM addFunc capabilities
    let errors = filter (not . isNothing) results
    case errors of 
        [] -> controller
        es  -> return $ head es
  where
    isNothing x = case x of
        Nothing -> True
        Just _  -> False
