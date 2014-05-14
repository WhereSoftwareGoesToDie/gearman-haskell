{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module System.Gearman.Protocol
(
    PacketMagic,
    PacketDomain,
    PacketType,
    PacketHeader,
    renderHeader,
    canDo,
    cantDo,
    resetAbilities,
    preSleep,
    unused1,
    noop,
    submitJob,
    jobCreated,
    grabJob,
    noJob,
    jobAssign,
    workStatusClient,
    workStatusWorker,
    workCompleteClient,
    workCompleteWorker,
    workFailClient,
    workFailWorker,
    getStatus,
    echoRes,
    submitJobBg,
    error,
    statusRes,
    submitJobHigh,
    setClientId,
    canDoTimeout,
    allYours,
    workExceptionClient,
    workExceptionWorker,
    optionReq,
    optionRes,
    workDataClient,
    workDataWorker,
    workWarningClient,
    workWarningWorker,
    grabJobUniq,
    jobAssignUniq,
    submitJobHighBg,
    submitJobLow,
    submitJobLowBg,
    submitJobSched,
    submitJobEpoch,
    packData,
    unpackData,
    buildEchoReq,
    buildCanDoReq,
    buildCanDoTimeoutReq
) where

import Prelude hiding (error)
import qualified Data.ByteString.Lazy as S
import Data.Binary.Put
import Control.Monad

data PacketMagic = Req | Res | Unused 

renderMagic :: PacketMagic -> S.ByteString
renderMagic Req = "\0REQ"
renderMagic Res = "\0REP"
renderMagic Unused = ""

data PacketDomain = Client | Worker | Both | None

data PacketType =   NullByte
                  | CanDo
                  | CantDo
                  | ResetAbilities
                  | PreSleep
                  | Unused1
                  | Noop
                  | SubmitJob
                  | JobCreated
                  | GrabJob
                  | NoJob
                  | JobAssign
                  | WorkStatus
                  | WorkComplete
                  | WorkFail
                  | GetStatus
                  | EchoReq
                  | EchoRes
                  | SubmitJobBg
                  | Error
                  | StatusRes
                  | SubmitJobHigh
                  | SetClientId
                  | CanDoTimeout
                  | AllYours
                  | WorkException
                  | OptionReq
                  | OptionRes
                  | WorkData
                  | WorkWarning
                  | GrabJobUniq
                  | JobAssignUniq
                  | SubmitJobHighBg
                  | SubmitJobLow
                  | SubmitJobLowBg
                  | SubmitJobSched
                  | SubmitJobEpoch
  deriving (Read,Show,Eq,Ord,Enum)

data PacketHeader = PacketHeader {
    packetType :: PacketType,
    magic      :: PacketMagic,
    domain     :: PacketDomain
}

renderHeader :: PacketHeader -> S.ByteString
renderHeader PacketHeader{..} =
    runPut $ do 
        putLazyByteString (renderMagic magic) 
        (putWord32be . fromIntegral . fromEnum) packetType

canDo               :: PacketHeader
canDo               = PacketHeader CanDo Req Worker

cantDo              :: PacketHeader
cantDo              = PacketHeader CantDo Req Worker

resetAbilities      :: PacketHeader
resetAbilities      = PacketHeader ResetAbilities Req Worker

preSleep            :: PacketHeader
preSleep            = PacketHeader PreSleep Req Worker

unused1             :: PacketHeader
unused1             = PacketHeader Unused1 Unused None

noop                :: PacketHeader
noop                = PacketHeader Noop Res Worker

submitJob           :: PacketHeader
submitJob           = PacketHeader SubmitJob Res Worker


jobCreated          :: PacketHeader
jobCreated          = PacketHeader JobCreated Req Client

grabJob             :: PacketHeader
grabJob             = PacketHeader GrabJob Res Client

noJob               :: PacketHeader
noJob               = PacketHeader NoJob Req Worker

jobAssign           :: PacketHeader
jobAssign           = PacketHeader JobAssign Res Worker

workStatusWorker    :: PacketHeader
workStatusWorker    = PacketHeader WorkStatus Req Worker

workStatusClient    :: PacketHeader
workStatusClient    = PacketHeader WorkStatus Res Client

workCompleteWorker  :: PacketHeader
workCompleteWorker  = PacketHeader WorkComplete Req Worker

workCompleteClient  :: PacketHeader
workCompleteClient  = PacketHeader WorkComplete Res Client

workFailWorker      :: PacketHeader
workFailWorker      = PacketHeader WorkFail Req Worker

workFailClient      :: PacketHeader
workFailClient      = PacketHeader WorkFail Res Client

getStatus           :: PacketHeader
getStatus           = PacketHeader GetStatus Req Client

echoReq             :: PacketHeader
echoReq             = PacketHeader EchoReq Req Both

echoRes             :: PacketHeader
echoRes             = PacketHeader EchoRes Res Both

submitJobBg         :: PacketHeader
submitJobBg         = PacketHeader SubmitJobBg Req Client

error               :: PacketHeader
error               = PacketHeader Error Res Both

statusRes           :: PacketHeader
statusRes           = PacketHeader StatusRes Res Client

submitJobHigh       :: PacketHeader
submitJobHigh       = PacketHeader SubmitJobHigh Req Client

setClientId         :: PacketHeader
setClientId         = PacketHeader SetClientId Req Worker

canDoTimeout        :: PacketHeader
canDoTimeout        = PacketHeader CanDoTimeout Req Worker

allYours            :: PacketHeader
allYours            = PacketHeader AllYours Req Worker

workExceptionWorker :: PacketHeader
workExceptionWorker = PacketHeader WorkException Req Worker

workExceptionClient :: PacketHeader
workExceptionClient = PacketHeader WorkException Res Client

optionReq           :: PacketHeader
optionReq           = PacketHeader OptionReq Req Both

optionRes           :: PacketHeader
optionRes           = PacketHeader OptionRes Res Both

workDataWorker      :: PacketHeader
workDataWorker      = PacketHeader WorkData Req Worker

workDataClient      :: PacketHeader
workDataClient      = PacketHeader WorkData Res Client

workWarningWorker   :: PacketHeader
workWarningWorker   = PacketHeader WorkWarning Req Worker

workWarningClient   :: PacketHeader
workWarningClient   = PacketHeader WorkWarning Res Client

grabJobUniq         :: PacketHeader
grabJobUniq         = PacketHeader GrabJobUniq Req Worker

jobAssignUniq       :: PacketHeader
jobAssignUniq       = PacketHeader JobAssignUniq Res Worker

submitJobHighBg     :: PacketHeader
submitJobHighBg     = PacketHeader SubmitJobHighBg Req Client

submitJobLow        :: PacketHeader
submitJobLow        = PacketHeader SubmitJobLow Req Client

submitJobLowBg      :: PacketHeader
submitJobLowBg      = PacketHeader SubmitJobLowBg Req Client

submitJobSched      :: PacketHeader
submitJobSched      = PacketHeader SubmitJobSched Req Client

submitJobEpoch      :: PacketHeader
submitJobEpoch      = PacketHeader SubmitJobEpoch Req Client

-- | marshalWord32 returns the ByteString representation of 
--   the supplied Int as a big-endian Word32. 
marshalWord32       :: Int -> S.ByteString
marshalWord32 n     = runPut $ putWord32be $ fromIntegral n

-- | packData takes a list of message parts (ByteStrings) and concatenates 
--   them with null bytes in between and the length in front.
packData            :: [S.ByteString] -> S.ByteString
packData d          = runPut $ do
    putWord32be $ fromIntegral $ S.length (toPayload d)
    putLazyByteString $ toPayload d
  where
    toPayload s = S.append (S.intercalate "\0" s) "\0"

-- | unpackData takes the data segment of a Gearman message (after the size 
-- word) and returns a list of the message arguments.
unpackData          :: S.ByteString -> [S.ByteString]
unpackData          = S.split '\0'

buildEchoReq        :: [S.ByteString] -> S.ByteString
buildEchoReq        = (S.append (renderHeader echoReq)) . packData

buildCanDoReq       :: S.ByteString -> S.ByteString
buildCanDoReq       = (S.append (renderHeader canDo)) . packData . (:[])

buildCanDoTimeoutReq :: 
    S.ByteString -> 
    Int -> 
    S.ByteString
buildCanDoTimeoutReq fn t = S.append (renderHeader canDoTimeout) (packData [fn, (marshalWord32 t)])
