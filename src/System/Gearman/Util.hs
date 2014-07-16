-- This file is part of gearman-haskell.
--
-- Copyright 2014 Anchor Systems Pty Ltd and others.
-- 
-- The code in this file, and the program it is a part of, is made
-- available to you by its authors as open source software: you can
-- redistribute it and/or modify it under the terms of the BSD license.

module System.Gearman.Util(
    lazyToChar8,
    char8ToLazy
) where

import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC

-- |lazyToChar8 converts a Lazy ByteString to a Char8 ByteString.
lazyToChar8 :: BL.ByteString -> BC.ByteString
lazyToChar8 = BC.concat . BL.toChunks

-- |char8ToLazy converts a Char8 ByteString to a Lazy ByteString.
char8ToLazy :: BC.ByteString -> BL.ByteString
char8ToLazy = BL.fromChunks . (:[])
