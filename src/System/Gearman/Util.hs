-- This file is part of gearman-haskell.
--
-- Copyright 2014 Anchor Systems Pty Ltd and others.
-- 
-- The code in this file, and the program it is a part of, is made
-- available to you by its authors as open source software: you can
-- redistribute it and/or modify it under the terms of the BSD license.

module System.Gearman.Util(
    lazyToStrictByteString,
    strictToLazyByteString
) where

import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as BS

-- |lazyToStrictByteString converts a Lazy ByteString to a StrictByteString ByteString.
lazyToStrictByteString :: BL.ByteString -> BS.ByteString
lazyToStrictByteString = BS.concat . BL.toChunks

-- |strictToLazyByteString converts a StrictByteString ByteString to a Lazy ByteString.
strictToLazyByteString :: BS.ByteString -> BL.ByteString
strictToLazyByteString = BL.fromChunks . (:[])
