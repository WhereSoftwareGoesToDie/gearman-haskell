-- This file is part of gearman-haskell.
--
-- Copyright 2014 Anchor Systems Pty Ltd and others.
-- 
-- The code in this file, and the program it is a part of, is made
-- available to you by its authors as open source software: you can
-- redistribute it and/or modify it under the terms of the BSD license.

module System.Gearman.Job
(
    JobHandle,
    JobWarning,
    JobData,
    JobStatus
--    JobMsgPayload(..),
--    JobMessage(..)
) where

import qualified Data.ByteString.Lazy as L(ByteString)

{-    
newtype JobHandle = JobHandle {
    unJobHandle :: L.ByteString
}

newtype JobWarning = JobWarning {
    unJobWarning :: L.ByteString
}

newtype JobData = JobData {
    unJobData :: L.ByteString
}

-- |Represents a fraction completed. First element is the numerator, 
-- second is the denominator.
newtype JobStatus = JobStatus {
    unJobStatus :: (Int, Int) 
}
-}

type JobHandle = L.ByteString
type JobWarning = L.ByteString
type JobData = L.ByteString
type JobStatus = (Int, Int)


--data JobMsgPayload = WarningPayload JobWarning
--                   | DataPayload JobData
--                   | StatusPayload JobStatus

-- |A JobMessage is sent by a worker back to the server, where it can 
-- be queried for by clients.
--data JobMessage = JobMessage {
--    handle  :: JobHandle,
--    payload :: JobMsgPayload
--}

