{-# LANGUAGE OverloadedStrings #-}

module System.Gearman where

import Data.Word

import System.Gearman.Protocol
import System.Gearman.Error
import System.Gearman.Connection

data Gearman a = Gearman {
    conn :: Connection
}

testConnect :: String -> Word16 -> IO (Maybe GearmanError)
testConnect h p = do
    c <- connect h p
    case c of 
        Left x  -> return $ Just x
        Right x -> echo x
