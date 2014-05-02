{-# LANGUAGE OverloadedStrings #-}

module System.Gearman.Error
(
    GearmanError,
    WorkerError
) where

import Data.ByteString.Lazy

data ErrorCode = ErrorCode ByteString deriving (Show)

data ErrorMessage = ErrorMessage ByteString deriving (Show)

data GearmanError = GearmanError {
    code :: ErrorCode,
    message :: ErrorMessage
}

data WorkerError = WorkerError {
    error :: ErrorMessage,
    fn    :: ByteString
}
