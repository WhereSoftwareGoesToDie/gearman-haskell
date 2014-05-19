{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

-- Protocol documentation snippets from http://gearman.org/protocol/.

module System.Gearman.Protocol
(
    PacketMagic,
    PacketDomain,
    PacketType,
    PacketHeader,
    packData,
    unpackData,
    buildEchoReq,
    buildCanDoReq,
    buildCanDoTimeoutReq,
    buildWorkCompleteReq,
    buildWorkDataReq,
    buildWorkStatusReq,
    buildWorkWarningReq,
    buildWorkFailReq,
    buildWorkExceptionReq,
    buildGrabJobReq
) where

import Prelude hiding (error)
import qualified Data.ByteString.Lazy as S
import Data.Binary.Put

import qualified System.Gearman.Job as J

-- |Whether the packet is a request, response or an unused code.
--
-- >Communication happens between either a client and job server, or between a worker
-- >and job server. In either case, the protocol consists of packets
-- >containing requests and responses. All packets sent to a job server
-- >are considered requests, and all packets sent from a job server are
-- >considered responses.
--
-- (http://gearman.org/protocol/)
data PacketMagic = Req | Res | Unused 

-- |Whether the packet type is sent/received by a client, a worker, 
-- both, or is an unused code.
data PacketDomain = Client | Worker | Both | None

-- |Packet types, enumerated for 0-36 (and defined for {1-4,6-36}). 
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

-- |PacketHeader encapsulates all the information in a packet except 
-- for the data (and data size prefix). 
data PacketHeader = PacketHeader {
    packetType :: PacketType,
    magic      :: PacketMagic,
    domain     :: PacketDomain
}

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

-- |Encode the 'magic' part of the packet header.
renderMagic :: PacketMagic -> S.ByteString
renderMagic Req = "\0REQ"
renderMagic Res = "\0REP"
renderMagic Unused = ""

-- |Return the ByteString representation of a PacketHeader.
renderHeader :: PacketHeader -> S.ByteString
renderHeader PacketHeader{..} =
    runPut $ do 
        putLazyByteString (renderMagic magic) 
        (putWord32be . fromIntegral . fromEnum) packetType

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
unpackData          = (S.split . fromIntegral . fromEnum) '\0'

-- |Construct an ECHO_REQ packet.
buildEchoReq        :: [S.ByteString] -> S.ByteString
buildEchoReq        = (S.append (renderHeader echoReq)) . packData

-- |Construct a CAN_DO request packet (workers send these to register 
-- functions with the server). 
buildCanDoReq       :: S.ByteString -> S.ByteString
buildCanDoReq       = (S.append (renderHeader canDo)) . packData . (:[])

-- |Construct a CAN_DO_TIMEOUT packet (as above, but with a timeout value).
-- FIXME: confirm timestamp format.
buildCanDoTimeoutReq :: 
    S.ByteString -> 
    Int -> 
    S.ByteString
buildCanDoTimeoutReq fn t = S.append (renderHeader canDoTimeout) (packData [fn, (marshalWord32 t)])

-- |Construct a WORK_COMPLETE packet (sent by workers when they 
-- finish a job). 
buildWorkCompleteReq :: J.JobHandle -> J.JobData -> S.ByteString
buildWorkCompleteReq handle response = 
    (S.append (renderHeader workCompleteWorker)) $ packData [handle, response]

-- |Construct a WORK_FAIL packet (sent by workers to report a job 
-- failure with no accompanying data). 
buildWorkFailReq :: J.JobHandle -> S.ByteString
buildWorkFailReq = (S.append (renderHeader workFailWorker)) . packData . (:[])

-- |Construct a WORK_EXCEPTION packet (sent by workers to report a job
-- failure with accompanying exception data). 
buildWorkExceptionReq :: J.JobHandle -> S.ByteString -> S.ByteString
buildWorkExceptionReq handle msg = (S.append (renderHeader workFailWorker)) $ packData [handle, msg]

-- |Construct a WORK_DATA packet (sent by workers when they have
-- intermediate data to send for a job). 
buildWorkDataReq :: J.JobHandle -> J.JobData -> S.ByteString
buildWorkDataReq handle payload =
    (S.append (renderHeader workDataWorker)) $ packData [handle, payload]

-- |Construct a WORK_WARNING packet (same as above, but treated as a 
-- warning). 
buildWorkWarningReq :: J.JobHandle -> J.JobData -> S.ByteString
buildWorkWarningReq handle payload =
    (S.append (renderHeader workWarningWorker)) $ packData [handle, payload]

-- |Construct a WORK_STATUS packet (send by workers to inform the server
-- of the percentage of the job that has been completed). 
buildWorkStatusReq :: J.JobHandle -> J.JobStatus -> S.ByteString
buildWorkStatusReq handle status =
    (S.append (renderHeader workStatusWorker)) $ packData args
  where
    args = handle : (map marshalWord32 $ toList status)
    toList t = [fst t, snd t]

buildGrabJobReq :: S.ByteString
buildGrabJobReq = renderHeader grabJob
