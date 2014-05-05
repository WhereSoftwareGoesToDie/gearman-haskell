module TestSuite where

import Control.Monad
import Control.Applicative
import Control.Monad.IO.Class
import Data.Either
import Data.Maybe
import Test.Hspec 
import Test.Hspec.HUnit
import Test.HUnit

import System.Gearman
import System.Gearman.Connection
import System.Gearman.Protocol

testConnectivity = do
    c <- testConnectEcho "localhost" "4730"
    assertBool "connect failed" (isNothing c)
    return ()

suite :: Spec
suite = do
    describe "Connectivity" $ do 
        it "connects to a local gearman server on localhost:4730" $ do 
            testConnectivity
