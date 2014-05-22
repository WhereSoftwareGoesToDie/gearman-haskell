{-# LANGUAGE OverloadedStrings #-}

module System.Gearman.Error
(
    GearmanError,
    WorkerError,
    gearmanError,
    printError
) where

import qualified Data.ByteString.Lazy as S
import Data.String
import Data.Int
import System.IO

data ErrorCode = ErrorCode Int32 deriving (Show)

data ErrorMessage = ErrorMessage S.ByteString deriving (Show)

data GearmanError = GearmanError {
    code :: !ErrorCode,
    message :: !ErrorMessage
}

instance Show GearmanError where
    show e = show (message e)

data WorkerError = WorkerError {
    error :: !ErrorMessage,
    fn    :: !S.ByteString
}

gearmanError :: Int32 -> [Char] -> GearmanError
gearmanError code msg = GearmanError (ErrorCode code) (ErrorMessage (fromString msg))

printError :: GearmanError -> IO ()
printError = hPrint stderr
