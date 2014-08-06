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
    work
) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Monad
import Control.Monad.Reader
import qualified Data.ByteString.Lazy as L
import Data.Either
import Data.Map (Map)
import qualified Data.Map as M
import Data.Maybe

import System.Gearman.Error
import System.Gearman.Connection
import System.Gearman.Protocol
import System.Gearman.Job

-- |A function can either throw some sort of error, or return a successful payload
type JobResult = Either JobError L.ByteString

-- |A WorkerFunc is a callback defined by the worker, taking a Job and
-- returning either an error or some data.
type WorkerFunc = Job -> IO JobResult

-- Error returned by a job. A Nothing will be sent as a WORK_FAIL
-- packet; Just err will be sent as a WORK_EXCEPTION packet with err
-- as the data argument.
type JobError = Maybe L.ByteString

-- |Maintained by the controller, defines the mapping between
-- function identifiers read from the server and Haskell functions.
type FuncMap = Map L.ByteString Capability

-- |A Capability is something a worker can do.
data Capability = Capability {
    func    :: WorkerFunc,
    timeout :: Maybe Int
}

-- |The data passed to a worker function when running a job.
data Job = Job {
    jobData       :: L.ByteString,
    sendWarning   :: (L.ByteString -> IO (Maybe GearmanError)),
    sendData      :: (L.ByteString -> IO (Maybe GearmanError)),
    sendStatus    :: (JobStatus    -> IO (Maybe GearmanError)),
    sendException :: (L.ByteString -> IO (Maybe GearmanError)),
    sendFailure   :: (IO (Maybe GearmanError)),
    sendComplete  :: (L.ByteString -> IO (Maybe GearmanError))
}

-- |The data passed to a worker when a job is received.
data JobSpec = JobSpec {
    jobArg      :: L.ByteString,
    jobName     :: L.ByteString,
    jobFunc     :: WorkerFunc,
    jobHandle   :: L.ByteString
}

-- | Takes in a list of (identifier, implementation, maybe timeouts) triples
-- If registration fails for any of the functions, processFuncs returns a
-- list of error messages, one for each failed registration.
-- If registration succeeds for all functions, returns the aggregate funcMap
processFuncs :: [(L.ByteString, WorkerFunc, Maybe Int)] -> Gearman (Either [String] FuncMap)
processFuncs funcs = do
    (errors, funcMaps) <- (liftM partitionEithers) (mapM processFunc funcs)
    if (null errors) then
        return $ Right (M.unions funcMaps)
    else
        return $ Left errors
  where
    processFunc (ident, func, timeout) = do
        packet <- case timeout of
            Nothing -> return $ buildCanDoReq ident
            Just t  -> return $ buildCanDoTimeoutReq ident t
        let funcMap = M.singleton ident (Capability func timeout)
        resp <- sendPacket packet
        case resp of
            Nothing  -> return $ Right funcMap
            Just err -> return $ Left $ "function registration failed: " ++ err

-- |Executes the given JOB_ASSIGn packet
-- If anything fails at any stage, it returns the error message
-- Manages sending all job status/complete/etc. messages
-- Does NOT send another GRAB_JOB
completeJob :: GearmanPacket -> FuncMap -> Gearman (Maybe String)
completeJob pkt funcMap = do
    case parseJob pkt funcMap of
        Left err -> return $ Just err
        Right jobSpec@JobSpec{..} -> do
            job@Job{..} <- createJob jobSpec
            let Capability{..} = fromJust $ M.lookup jobName funcMap
            liftIO $ case timeout of
                Nothing -> do
                    result <- func job
                    handleResult job result
                Just t -> do
                        timer <- async (threadDelay (t * 1000000))
                        worker <- async (func job)
                        winner <- waitEither timer worker
                        either (\_ -> return Nothing) (handleResult job) winner
  where
    handleResult Job{..} (Left jobErr) = do
        case jobErr of
            Nothing -> sendFailure
            Just jobException -> sendException jobException
    handleResult Job{..} (Right payload) = sendComplete payload

-- |Builds a Job with the appropriate packet sending functions from a JobSpec
createJob :: JobSpec -> Gearman Job
createJob JobSpec{..} = do
    connection <- ask
    return $ Job jobArg
                 (sendPacketIO connection . buildWorkWarningReq jobHandle)
                 (sendPacketIO connection . buildWorkDataReq jobHandle)
                 (sendPacketIO connection . buildWorkStatusReq jobHandle)
                 (sendPacketIO connection . buildWorkExceptionReq jobHandle)
                 (sendPacketIO connection $ buildWorkFailReq jobHandle)
                 (sendPacketIO connection . buildWorkCompleteReq jobHandle)

-- |Parses a JobSpec from a JOB_ASSIGN packet and FuncMap
-- If the parsing fails or the requested function hasn't been registered
-- it returns an appropriate error message
-- Otherwise it returns the successfully parsed JobSpec
parseJob :: GearmanPacket -> FuncMap -> Either String JobSpec
parseJob GearmanPacket{..} funcMap =
    let PacketHeader{..} = header in
    case (parseSpecs args) of
        Nothing -> Left "not enough arguments provided to JOB_ASSIGN"
        Just (jobHandle, fnId, dataArg) -> case (M.lookup fnId funcMap) of
            Nothing -> Left $ "no function with name " ++ (show fnId)
            Just Capability{..} ->
                Right $ JobSpec dataArg fnId func jobHandle
  where
    parseSpecs (jobHandle:fnId:dataArg:_) = Just (jobHandle, fnId, dataArg)
    parseSpecs _                       = Nothing

-- | Entry point into this module
-- Takes in a list of (identifier, implementation, maybe timeouts) triples,
-- registers the functions parsed from the triples, then starts the main work loop
-- sends an initial GRAB_JOB packet before entering the loop
-- if the work loop returns an error/disconnection message or any of the setup fails
-- it returns the message
work :: [(L.ByteString, WorkerFunc, Maybe Int)] -> Gearman String
work funcs = do
    funcReg <- processFuncs funcs
    case funcReg of
        Left errs -> return $ concat ["Failed to register functions, errors: ", concat errs]
        Right funcMap -> do
            resp <- sendPacket buildGrabJobReq
            case resp of
                Just err -> return err
                Nothing -> loop funcMap

-- | The core work loop.
-- Returns a String when it terminates, stating why it terminated
loop :: FuncMap -> Gearman String
loop funcMap = do
    incoming <- recvPacket DomainWorker
    case incoming of
        Left err -> return err
        Right pkt@GearmanPacket{..} -> do
            let PacketHeader{..} = header
            case packetType of
                NoJob         -> do
                    resp <- sendPacket buildPreSleepReq
                    handleResponse resp
                Noop          -> do
                    resp <- sendPacket buildGrabJobReq
                    handleResponse resp
                JobAssign     -> do
                    result <- completeJob pkt funcMap
                    case result of
                        Nothing  -> do
                            resp <- sendPacket buildGrabJobReq
                            handleResponse resp
                        Just err -> return err
                wat           -> return $ "Unexpected packet of type " ++ (show wat)
  where
    handleResponse (Just err) = return err
    handleResponse Nothing    = loop funcMap
