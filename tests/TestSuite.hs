{-# LANGUAGE OverloadedStrings #-}

module TestSuite where

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
    result <- runGearman "localhost" "4730" $ do
        workerRes <- runWorker 2 (runWorkAsync (registerHello >> work))
        liftIO $ putStrLn $ show workerRes
        requestWork
        a <- receiveResponse
        liftIO $ putStrLn $ show a
        b <- receiveResponse
        liftIO $ putStrLn $ show b
        c <- receiveResponse
        liftIO $ putStrLn $ show c

    putStrLn $ concat ["Returned: ", show result]    

    return ()

workerGrabJob = do
    liftIO $ putStrLn "Worker asking for work"
    let request = buildGrabJobReq
    requestResponse <- sendPacket request
    if (isJust requestResponse)
    then liftIO $ putStrLn $ concat ["Asked server for work and it derped: ", show (fromJust requestResponse)]
    else return ()


requestWork = do
    liftIO $ putStrLn "Sending Work Request:"
    let request = buildSubmitJob "hello" "" "HITHERE"
    liftIO $ putStrLn $ prettyHex $ L.toStrict request
    requestResponse <- sendPacket request
    if (isJust requestResponse)
    then liftIO $ putStrLn $ concat ["Asked server to run me hello and it derped: ", show (fromJust requestResponse)] 
     else return ()

receiveResponse = do 
    liftIO $ putStrLn "Receiving Response"
    creationResponse <- recvPacket DomainClient 
    case creationResponse of
        Left e -> do
            liftIO $ putStrLn $ concat ["Asked server to gimme ma response and it derped: ", show e] 
            return []
        Right (GearmanPacket _ _ handle) -> do
            return handle

registerHello :: Worker (Maybe GearmanError)
registerHello = addFunc "hello" helloFunc Nothing

helloFunc :: WorkerFunc
helloFunc _ = return $ Right "Hello World"

--testRegister = do
--    r <- runGearman "localhost" "4370" $ return $ addFunc "reverse" rvs Nothing
--    assertBool "send failed" (isNothing r)
--    return ()

suite :: Spec
suite = do
    describe "Connectivity" $ do 
        it "connects to a local gearman server on localhost:4730" $ do 
            testConnectivity
    describe "Basics" $ do
        it "can Hello World!" $ do
            testHello
