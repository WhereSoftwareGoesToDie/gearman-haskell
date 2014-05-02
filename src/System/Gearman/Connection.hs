module Gearman.Connection where

import Control.Monad
import Control.Applicative
import Control.Monad.Trans
import Control.Monad.State.Class
import Network.Socket hiding (recv)
import Network.Socket.ByteString

import System.Gearman.Error

data Connection = Connection {
    sock :: Socket
}

