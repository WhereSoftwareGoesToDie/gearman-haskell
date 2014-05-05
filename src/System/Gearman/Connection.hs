{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module System.Gearman.Connection where

import Prelude hiding (length)
import Control.Monad
import Control.Applicative
import Control.Monad.Trans
import Control.Monad.State.Class
import qualified Network.Socket as N
import Network.Socket.ByteString
import Data.ByteString.Char8 as S
import Data.Word
import Data.Int
import Data.String
import Data.Either
import Foreign.C.Types
import GHC.IO.Handle

import System.Gearman.Error
import System.Gearman.Protocol
import System.Gearman.Util

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

-- | connect attempts to connect to the supplied hostname and port.
connect :: String -> Word16 -> IO (Either GearmanError Connection)
connect host port = do
    sock <- N.socket N.AF_INET N.Datagram N.defaultProtocol
    ai <- getHostAddress host port
    case ai of 
        Nothing -> return $ Left $ gearmanError 1 (fromString ("could not resolve address" ++ host))
        Just x  -> do
            N.connect sock $ N.addrAddress x 
            return $ Right $ Connection sock

-- | echo tests a Connection by sending (and waiting for a response to) an
--   echo packet. 
echo :: Connection -> IO (Maybe GearmanError)
echo Connection{..} = do
    sent <- send sock $ lazyToChar8 req
    let expected = fromIntegral (length $ lazyToChar8 $ req)  
    case () of _
                 | (sent == expected) -> do
                   rep <- recv sock 8
                   case (S.length $ rep) of 
                       8 -> return Nothing
                       x -> return $ Just $ recvError x
                 | otherwise          -> return $ Just $ sendError sent
  where
    req = renderHeader echoReq
    sendError b = gearmanError 2 ("echo failed: only sent " ++ (show b) ++ " bytes")
    recvError b = gearmanError 3 ("echo failed: only received " ++ (show b) ++ " bytes")
