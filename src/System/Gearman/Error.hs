-- This file is part of gearman-haskell.
--
-- Copyright 2014 Anchor Systems Pty Ltd and others.
-- 
-- The code in this file, and the program it is a part of, is made
-- available to you by its authors as open source software: you can
-- redistribute it and/or modify it under the terms of the BSD license.

{-# LANGUAGE OverloadedStrings #-}

module System.Gearman.Error
(
    GearmanError,
    printError
) where

import System.IO

type GearmanError = String

printError :: GearmanError -> IO ()
printError = hPrint stderr
