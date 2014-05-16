module System.Gearman.Job
(
    JobHandle,
    JobWarning,
    JobData,
    JobStatus,
    JobMsgPayload(..),
    JobMessage(..)
) where

import qualified Data.ByteString.Lazy as S
import Data.ByteString.Lazy (ByteString)
    
type JobHandle = S.ByteString

type JobWarning = S.ByteString

type JobData = S.ByteString

-- |Represents a fraction completed. First element is the numerator, 
-- second is the denominator.
type JobStatus = (Int, Int)

data JobMsgPayload = JobWarning | JobData | JobStatus

data JobMessage = JobMessage {
    handle :: JobHandle,
    payload :: JobMsgPayload
}

