{-# LANGUAGE OverloadedStrings #-}

module TestSuite where

import Control.Monad
import Control.Applicative
import Control.Monad.IO.Class
import Data.Either
import Data.Maybe
import Test.Hspec 
import Test.Hspec.HUnit
import Test.HUnit
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy  as L

import System.Gearman
import System.Gearman.Error
import System.Gearman.Connection
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

testHelloWorld = do
    response <- liftIO $ connect "localhost" "4730"
    assertBool "connect failed" $ isLeft response
--    let (Right connection) = response
--    funcRegResponse <- addFunc "helloWorld" helloWorld Nothing
    return ()
  where
    helloWorld :: Job -> IO (Either JobError L.ByteString)
    helloWorld _ = return $ Right "Nin hao, shi jie"

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
        it "sends a Hello World!" $ pendingWith "testHelloWorld NYI"
        it "sends back a packet N times" $ pendingWith "Many packets test"
        it "sends back a packet with N*concat payload" $ pendingWith "Big packet test"
