-- This file is part of gearman-haskell.
--
-- Copyright 2014 Anchor Systems Pty Ltd and others.
-- 
-- The code in this file, and the program it is a part of, is made
-- available to you by its authors as open source software: you can
-- redistribute it and/or modify it under the terms of the BSD license.

{-# LANGUAGE OverloadedStrings #-}

module System.Gearman where

import System.Gearman.Error
import System.Gearman.Connection

newtype Gearman = Gearman {
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
