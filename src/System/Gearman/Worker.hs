module System.Gearman.Worker
(
    Job
) where

import Data.ByteString.Lazy

data Job = Job {
    jobData      :: [ByteString],
    fn           :: ByteString,
    sendWarning  :: (ByteString -> ()),
    sendData     :: (ByteString -> ()),
    updateStatus :: (Int -> Int -> ()),
    handle       :: ByteString,
    uniqId       :: ByteString
}
