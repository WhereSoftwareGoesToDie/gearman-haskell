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
import Data.ByteString(ByteString)
import qualified Data.ByteString.Char8  as S
import qualified Data.ByteString.Lazy   as L
import qualified Data.ByteString.Base64 as B64

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

testHello :: IO ()
testHello = do
    _ <- registerFuncsAndWork [registerHelloTriple]
    result <- newEmptyMVar
    requestWork "hello" "" "" result
    result' <- readMVar result
    (Right expected) <- helloFunc undefined
    checkMatch (result' !! 1) expected
    return ()

testReverse :: IO ()
testReverse = replicateM_ 1 $ do
    _ <- registerFuncsAndWork [registerReverseTriple]
    result <- newEmptyMVar
    let input = "this is a string"
    requestWork "reverse" "" input result
    result' <- readMVar result
    let expected = L.reverse input
    checkMatch (result' !! 1)  expected
    return ()

testGoodNagios :: IO ()
testGoodNagios = do    
    maBox <- newEmptyMVar
    let maString  = B64.encode "\"host_name=kvm33.syd1.anchor.net.au\ncore_start_time=1405044854.0\nstart_time=1405044867.223223\nfinish_time=1405044867.347834\nreturn_code=0\nexited_ok=1\nservice_description=procs\noutput=PROCS OK: 796 processes |procs=796;;\\n\n\n\n\n\"" :: ByteString
    requestWork "testfunc" "" (L.fromStrict maString) maBox
    maResult <- readMVar maBox
    putStrLn $ concat ["Ma result was: ", show maResult]

apply :: [(a -> m b)]-> [a] -> [m b]
apply [] _ = []
apply _ [] = []
apply (f:fs) (x:xs) = (f x):(apply fs xs)

checkMatch expected received = assertBool (concat ["incorrect output, expected: ", show expected, " , received: ", show received]) (expected == received)

registerFuncsAndWork funcRegTriples = forkIO $ do
        runGearman "localhost" "4730" $ work funcRegTriples >> return ()

requestWork funcName uniqId args resultBox = do
    forkIO $ runGearman "localhost" "4730" $ do
        let request = buildSubmitJob funcName uniqId args
        sendPacket request
        _ <- receiveResponse
        ret  <- receiveResponse
        liftIO $ putMVar resultBox ret
    return ()

receiveResponse = do 
    creationResponse <- recvPacket DomainClient 
    case creationResponse of
        Left e -> do
            liftIO $ putStrLn $ concat ["Asked server to give a response and it died: ", show e] 
            return []
        Right (GearmanPacket _ _ handle) -> do
            return handle

registerHelloTriple  = ("hello", helloFunc, Nothing)
registerReverseTriple = ("reverse", reverseFunc, Nothing)

helloFunc :: WorkerFunc
helloFunc _ = return $ Right "Hello World"

reverseFunc :: WorkerFunc
reverseFunc Job{jobData = jd} = return $ Right $ L.reverse jd

suite :: Spec
suite = do
-- This test is commented out for travis (it doesn't run a nagios gearman collector)
--    describe "Nagios Perfdata" $ do
--        it "dos that thing" $ do
--            testGoodNagios
    describe "Connectivity" $ do 
        it "connects to a local gearman server on localhost:4730" $ do 
            testConnectivity
    describe "Basics - no parameter funcs" $ do
        it "can Hello World!" $ do
            testHello
    describe "Basics - one parameter funcs" $ do            
        it "can reverse strings" $ do
            testReverse
