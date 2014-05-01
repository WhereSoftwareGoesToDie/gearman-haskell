module Gearman.Protocol
(
    PacketMagic
    PacketDomain
    PacketType
    PacketHeader
) where

import Data.ByteString as S

data PacketMagic = Req | Res | Unused 

renderMagic :: PacketMagic -> ByteString
renderMagic Req = S.pack "REQ\0"
renderMagic Rep = S.pack "REP\0"
renderMagic Unused = S.pack ""

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

