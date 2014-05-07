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

import System.Gearman
import System.Gearman.Connection
import System.Gearman.Protocol
import System.Gearman.Worker

testConnectivity = do
    c <- testConnectEcho "localhost" "4730"
    assertBool "connect failed" (isNothing c)
    return ()

--testRegister = do
--    r <- runGearman "localhost" "4370" $ return $ addFunc "reverse" rvs Nothing
--    assertBool "send failed" (isNothing r)
--    return ()

suite :: Spec
suite = do
    describe "Connectivity" $ do 
        it "connects to a local gearman server on localhost:4730" $ do 
            testConnectivity
