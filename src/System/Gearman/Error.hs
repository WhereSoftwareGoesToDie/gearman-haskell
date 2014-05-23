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
