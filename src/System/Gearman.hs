{-# LANGUAGE OverloadedStrings #-}

module System.Gearman where

import System.Gearman.Error
import System.Gearman.Connection

data Gearman a = Gearman {
    conn :: Connection
}

-- | testConnectEcho connects to the provided host and port, sends a simple 
--   ECHO_REQ packet, and verifies the result. Returns Nothing on success,
--   Just GearmanError on failure.
testConnectEcho :: String -> String -> IO (Maybe GearmanError)
testConnectEcho h p = do
    c <- connect h p
    case c of 
        Left x  -> return $ Just x
        Right x -> echo x ["ping"]
