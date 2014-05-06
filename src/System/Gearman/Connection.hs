{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module System.Gearman.Connection(
    Connection(..),
    connect,
    echo,
    runGearman,
    sendPacket
) where

import Prelude hiding (length)
import Control.Exception
import Control.Monad
import Control.Applicative
import Control.Monad.Trans
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.State.Class
import qualified Network.Socket as N
import Network.Socket.ByteString
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as L
import Data.Word
import Data.Int
import Data.String
import Data.Either
import Foreign.C.Types
import GHC.IO.Handle

import System.Gearman.Error
import System.Gearman.Protocol hiding (error)
import System.Gearman.Util

data Connection = Connection {
    sock :: N.Socket
}

newtype Gearman a = Gearman (ReaderT Connection IO a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader Connection)

getHostAddress :: String -> String -> IO (Maybe N.AddrInfo)
getHostAddress host port = do
    ai <- N.getAddrInfo hints (Just host) (Just port)
    case ai of 
        (x:_) -> return $ Just x
        [] -> return Nothing
  where
    hints = Just $ N.defaultHints { 
        N.addrProtocol = 6,
        N.addrFamily   = N.AF_INET,
        N.addrFlags    = [ N.AI_NUMERICSERV ]
    }

-- | connect attempts to connect to the supplied hostname and port.
connect :: String -> String -> IO (Either GearmanError Connection)
connect host port = do
    sock <- N.socket N.AF_INET N.Stream 6 -- Create new ipv4 TCP socket.
    ai <- getHostAddress host port
    case ai of 
        Nothing -> return $ Left $ gearmanError 1 (fromString ("could not resolve address" ++ host))
        Just x  -> do
            N.connect sock $ (N.addrAddress x)
            return $ Right $ Connection sock

-- | echo tests a Connection by sending (and waiting for a response to) an
--   echo packet. 
echo :: Connection -> [L.ByteString] ->  IO (Maybe GearmanError)
echo Connection{..} payload = do
    sent <- send sock $ lazyToChar8 req
    let expected = fromIntegral (S.length $ lazyToChar8 $ req)  
    case () of _
                 | (sent == expected) -> do
                   rep <- recv sock 8
                   case (S.length $ rep) of 
                       8 -> return Nothing
                       x -> return $ Just $ recvError x
                 | otherwise          -> return $ Just $ sendError sent
  where
    req = buildEchoReq payload
    sendError b = gearmanError 2 ("echo failed: only sent " ++ (show b) ++ " bytes")
    recvError b = gearmanError 3 ("echo failed: only received " ++ (show b) ++ " bytes")

cleanup :: Connection -> IO ()
cleanup Connection{..} = N.sClose sock

runGearman :: String -> String -> Gearman a -> IO a
runGearman host port (Gearman action) = do
    c <- connect host port
    case c of 
        Left x  -> (error (show x))
        Right x -> do
            r <- runReaderT action x
            liftIO $ cleanup x
            return r

sendPacket :: L.ByteString -> Gearman (Maybe GearmanError)
sendPacket packet = do
    Connection{..} <- ask
    let expected = fromIntegral (S.length $ lazyToChar8 packet)
    sent <- liftIO $ send sock $ lazyToChar8 packet
    case () of _
                 | (sent == expected) -> return Nothing
                 | otherwise          -> return $ Just $ sendError sent
  where 
    sendError b = gearmanError 2 ("send failed: only sent " ++ (show b) ++ " bytes")
