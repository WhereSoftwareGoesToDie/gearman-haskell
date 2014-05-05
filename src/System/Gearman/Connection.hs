{-# LANGUAGE OverloadedStrings #-}

module Gearman.Connection where

import Control.Monad
import Control.Applicative
import Control.Monad.Trans
import Control.Monad.State.Class
import qualified Network.Socket as N
import Network.Socket.ByteString
import Data.ByteString.Lazy as S
import Data.Word
import Data.Int
import Data.String
import Data.Either
import Foreign.C.Types
import GHC.IO.Handle

import System.Gearman.Error

data Connection = Connection {
    sock :: N.Socket
}

getHostAddress :: String -> Word16 -> IO (Maybe N.AddrInfo)
getHostAddress host port = do
    ai <- N.getAddrInfo hints (Just host) Nothing
    case ai of 
        (x:_) -> return $ Just x
        [] -> return Nothing
  where
    hints = Just $ N.defaultHints { N.addrProtocol = (fromIntegral port) }

connect :: String -> Word16 -> IO (Either GearmanError Connection)
connect host port = do
    sock <- N.socket N.AF_INET N.Datagram N.defaultProtocol
    ai <- getHostAddress host port
    case ai of 
        Nothing -> return $ Left $ GearmanError 1 (fromString ("could not resolve address" ++ host))
        Just x  -> do
            N.connect sock $ N.addrAddress x 
            return $ Right $ Connection sock
