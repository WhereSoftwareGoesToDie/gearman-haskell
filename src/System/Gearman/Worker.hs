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
    timeout :: Maybe Int,
    outgoing :: TBChan Job
}

newCapability :: S.ByteString -> 
              (Job -> IO (Either JobError S.ByteString)) -> 
              Maybe Int ->
              Worker Capability
newCapability ident f timeout = do
    outChan <- liftIO $ atomically $ newTBChan 1
    return $ Capability ident (WorkerFunc f) timeout outChan

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

addFunc :: S.ByteString ->
           (Job -> IO (Either JobError S.ByteString)) -> 
           Maybe Int ->
           Worker (Either GearmanError Capability)
addFunc name f tout = do
    Work{..} <- get
    cap <- newCapability name f tout
    let Capability{..} = cap
    let packet = case timeout of
                    Nothing -> buildCanDoReq ident
                    Just t  -> buildCanDoTimeoutReq ident t
    put $ Work (M.insert ident cap workMap) nWorkers
    res <- liftGearman $ sendPacket packet
    case res of
        Nothing -> return $ Right cap
        Just e  -> return $ Left e

controller :: Worker (Maybe GearmanError)
controller = do
    Work{..} <- get
    case (M.null workMap) of
        True -> return Nothing -- nothing to do, so exit without error
        False -> undefined

