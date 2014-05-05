module Gearman.Connection where

import Control.Monad
import Control.Applicative
import Control.Monad.Trans
import Control.Monad.State.Class
import qualified Network.Socket as N
import Network.Socket.ByteString
import Data.ByteString as S
import Data.Word
import Data.Int
import Foreign.C.Types
import GHC.IO.Handle

import System.Gearman.Error

data Connection = Connection {
    h :: Handle
}

getHostAddress :: String -> Word16 -> IO N.AddrInfo
getHostAddress host port = do
    ai <- N.getAddrInfo hints (Just host) Nothing
    case ai of 
        (x:_) -> return x
        [] -> return N.defaultHints
  where
    hints = Just $ N.defaultHints { N.addrProtocol = (fromIntegral port) }

connect :: String -> Word16 -> IO N.Socket
connect host port = do
    sock <- N.socket N.AF_INET N.Datagram N.defaultProtocol
    ai <- getHostAddress host port
    N.connect sock $ N.addrAddress $ ai
    return sock
