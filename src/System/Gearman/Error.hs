{-# LANGUAGE OverloadedStrings #-}

module System.Gearman.Error
(
    GearmanError,
    WorkerError,
    gearmanError
) where

import qualified Data.ByteString.Lazy as S
import Data.String
import Data.Int

data ErrorCode = ErrorCode Int32 deriving (Show)

data ErrorMessage = ErrorMessage S.ByteString

data GearmanError = GearmanError {
    code :: !ErrorCode,
    message :: !ErrorMessage
}

data WorkerError = WorkerError {
    error :: !ErrorMessage,
    fn    :: !S.ByteString
}

gearmanError :: Int32 -> [Char] -> GearmanError
gearmanError code msg = GearmanError (ErrorCode code) (ErrorMessage (fromString msg))
