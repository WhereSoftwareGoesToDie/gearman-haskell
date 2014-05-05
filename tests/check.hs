module Main where

import Test.Hspec 
import Test.HUnit

import TestSuite

main :: IO ()
main = do
    hspec suite
