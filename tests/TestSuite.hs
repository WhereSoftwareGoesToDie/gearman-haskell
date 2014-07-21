{-# LANGUAGE OverloadedStrings #-}

module TestSuite where

import Control.Concurrent
import Control.Monad
import Control.Applicative
import Control.Monad.IO.Class
import Data.Either
import Data.Maybe
import Hexdump
import Test.Hspec 
import Test.Hspec.HUnit
import Test.HUnit
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy  as L

import System.Gearman.Connection
import System.Gearman.Error
import System.Gearman.Protocol
import System.Gearman.Worker

-- | testConnectEcho connects to the provided host and port, sends a simple 
--   ECHO_REQ packet, and verifies the result. Returns Nothing on success,
--   Just GearmanError on failure.
testConnectEcho :: String -> String -> IO (Maybe GearmanError)
testConnectEcho h p = do
    c <- connect h p
    case c of 
        Left x  -> return $ Just x
        Right x -> echo x ["ping"]

testConnectivity = do
    c <- testConnectEcho "localhost" "4730"
    assertBool "connect failed" (isNothing c)
    return ()


testHello = do
    registerFuncsAndWork 1 [registerHello]
    result <- newEmptyMVar
    requestWork "hello" "" "" result
    result' <- readMVar result
    (Right expected) <- helloFunc undefined
    checkMatch result' expected
    return ()

testReverse :: IO ()
testReverse = do
    registerFuncsAndWork 1 [registerReverse]
    result <- newEmptyMVar
    let input = "this is a string"
    requestWork "reverse" "" input result
    result' <- readMVar result
    let expected = L.reverse input
    checkMatch result' expected
    return ()

checkMatch expected received = assertBool (concat ["incorrect output, expected: ", show expected, " , received: ", show received]) (expected == received)

registerFuncsAndWork n funcRegs = do
    forkIO $ runGearman "localhost" "4730" $ do
        runWorker n ((sequence_ funcRegs) >> work) >> return ()

requestWork funcName uniqId args resultBox = do
    forkIO $ runGearman "localhost" "4730" $ do
        let request = buildSubmitJob funcName uniqId args
        sendPacket request
        _ <- receiveResponse
        ret  <- receiveResponse
        liftIO $ putMVar resultBox (ret !! 1)

receiveResponse = do 
    creationResponse <- recvPacket DomainClient 
    case creationResponse of
        Left e -> do
            liftIO $ putStrLn $ concat ["Asked server to give a response and it died: ", show e] 
            return []
        Right (GearmanPacket _ _ handle) -> do
            return handle

registerHello :: Worker (Maybe GearmanError)
registerHello = addFunc "hello" helloFunc Nothing

registerReverse :: Worker (Maybe GearmanError)
registerReverse = addFunc "reverse" reverseFunc Nothing

helloFunc :: WorkerFunc
helloFunc _ = return $ Right "Hello World"

reverseFunc :: WorkerFunc
reverseFunc (Job jd _ _ _) = return $ Right $ L.reverse jd

suite :: Spec
suite = do
    describe "Connectivity" $ do 
        it "connects to a local gearman server on localhost:4730" $ do 
            testConnectivity
    describe "Basics - no parameter funcs" $ do
        it "can Hello World!" $ do
            testHello
    describe "Basics - one parameter funcs" $ do            
        it "can reverse strings" $ do
            testReverse
