module System.Gearman.Util where

import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC

lazyToChar8 :: BL.ByteString -> BC.ByteString
lazyToChar8 = BC.concat . BL.toChunks
