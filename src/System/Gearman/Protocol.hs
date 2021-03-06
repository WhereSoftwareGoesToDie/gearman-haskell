-- This file is part of gearman-haskell.
--
-- Copyright 2014 Anchor Systems Pty Ltd and others.
--
-- The code in this file, and the program it is a part of, is made
-- available to you by its authors as open source software: you can
-- redistribute it and/or modify it under the terms of the BSD license.

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

-- Protocol documentation snippets from http://gearman.org/protocol/.
-- The above is out of date; as far as I can tell the current protocol
-- 'documentation' is the header files in the gearman source
-- distribution.

module System.Gearman.Protocol
(
    PacketMagic(..),
    PacketDomain(..),
    PacketType(..),
    GearmanPacket(..),
    PacketHeader(..),
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
    buildGrabJobReq,
    buildGrabJobUniqReq,
    buildPreSleepReq,
    buildNoopRes,
    buildSubmitJob,
    parseDataSize,
    parsePacket
) where

import Prelude hiding (error)
import qualified Data.ByteString.Lazy as L
import Data.Word
import Data.Binary.Put
import Data.Binary.Get
import Hexdump

import qualified System.Gearman.Job as J
import System.Gearman.Error

-- |Whether the packet is a request, response or an unused code.
--
-- >Communication happens between either a client and job server, or between a worker
-- >and job server. In either case, the protocol consists of packets
-- >containing requests and responses. All packets sent to a job server
-- >are considered requests, and all packets sent from a job server are
-- >considered responses.
--
-- (http://gearman.org/protocol/)
data PacketMagic = Req | Res | UnknownMagic
    deriving Show
-- |Whether the packet type is sent/received by a client, a worker,
-- both, or is an unused code.
data PacketDomain = DomainClient | DomainWorker | DomainBoth | DomainNone
    deriving Show

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

fromWord32 :: Word32 -> Either GearmanError PacketType
fromWord32 1  = Right CanDo
fromWord32 2  = Right CantDo
fromWord32 3  = Right ResetAbilities
fromWord32 4  = Right PreSleep
-- 5 is unused
fromWord32 6  = Right Noop
fromWord32 7  = Right SubmitJob
fromWord32 8  = Right JobCreated
fromWord32 9  = Right GrabJob
fromWord32 10 = Right NoJob
fromWord32 11 = Right JobAssign
fromWord32 12 = Right WorkStatus
fromWord32 13 = Right WorkComplete
fromWord32 14 = Right WorkFail
fromWord32 15 = Right GetStatus
fromWord32 16 = Right EchoReq
fromWord32 17 = Right EchoRes
fromWord32 18 = Right SubmitJobBg
fromWord32 19 = Right Error
fromWord32 20 = Right StatusRes
fromWord32 21 = Right SubmitJobHigh
fromWord32 22 = Right SetClientId
fromWord32 23 = Right CanDoTimeout
fromWord32 24 = Right AllYours
fromWord32 25 = Right WorkException
fromWord32 26 = Right OptionReq
fromWord32 27 = Right OptionRes
fromWord32 28 = Right WorkData
fromWord32 29 = Right WorkWarning
fromWord32 30 = Right GrabJobUniq
fromWord32 31 = Right JobAssignUniq
fromWord32 32 = Right SubmitJobHighBg
fromWord32 33 = Right SubmitJobLow
fromWord32 34 = Right SubmitJobLowBg
fromWord32 35 = Right SubmitJobSched
fromWord32 36 = Right SubmitJobEpoch
fromWord32 n = Left $ "invalid packet type: " ++ show n

-- |PacketHeader encapsulates all the information in a packet except
-- for the data (and data size prefix).
data PacketHeader = PacketHeader {
    packetType :: PacketType,
    magic      :: PacketMagic,
    domain     :: PacketDomain
} deriving Show

-- |Full Gearman packet.
data GearmanPacket = GearmanPacket {
    header   :: PacketHeader,
    dataSize :: Int,
    args     :: [L.ByteString]
} deriving Show

canDo               :: PacketHeader
canDo               = PacketHeader CanDo Req DomainWorker

cantDo              :: PacketHeader
cantDo              = PacketHeader CantDo Req DomainWorker

resetAbilities      :: PacketHeader
resetAbilities      = PacketHeader ResetAbilities Req DomainWorker

preSleep            :: PacketHeader
preSleep            = PacketHeader PreSleep Req DomainWorker

unused1             :: PacketHeader
unused1             = PacketHeader Unused1 UnknownMagic DomainNone

noop                :: PacketHeader
noop                = PacketHeader Noop Res DomainWorker

submitJob           :: PacketHeader
submitJob           = PacketHeader SubmitJob Req DomainClient

jobCreated          :: PacketHeader
jobCreated          = PacketHeader JobCreated Req DomainClient

grabJob             :: PacketHeader
grabJob             = PacketHeader GrabJob Req DomainWorker

noJob               :: PacketHeader
noJob               = PacketHeader NoJob Req DomainWorker

jobAssign           :: PacketHeader
jobAssign           = PacketHeader JobAssign Res DomainWorker

workStatusWorker    :: PacketHeader
workStatusWorker    = PacketHeader WorkStatus Req DomainWorker

workStatusClient    :: PacketHeader
workStatusClient    = PacketHeader WorkStatus Res DomainClient

workCompleteWorker  :: PacketHeader
workCompleteWorker  = PacketHeader WorkComplete Req DomainWorker

workCompleteClient  :: PacketHeader
workCompleteClient  = PacketHeader WorkComplete Res DomainClient

workFailWorker      :: PacketHeader
workFailWorker      = PacketHeader WorkFail Req DomainWorker

workFailClient      :: PacketHeader
workFailClient      = PacketHeader WorkFail Res DomainClient

getStatus           :: PacketHeader
getStatus           = PacketHeader GetStatus Req DomainClient

echoReq             :: PacketHeader
echoReq             = PacketHeader EchoReq Req DomainBoth

echoRes             :: PacketHeader
echoRes             = PacketHeader EchoRes Res DomainBoth

submitJobBg         :: PacketHeader
submitJobBg         = PacketHeader SubmitJobBg Req DomainClient

error               :: PacketHeader
error               = PacketHeader Error Res DomainBoth

statusRes           :: PacketHeader
statusRes           = PacketHeader StatusRes Res DomainClient

submitJobHigh       :: PacketHeader
submitJobHigh       = PacketHeader SubmitJobHigh Req DomainClient

setClientId         :: PacketHeader
setClientId         = PacketHeader SetClientId Req DomainWorker

canDoTimeout        :: PacketHeader
canDoTimeout        = PacketHeader CanDoTimeout Req DomainWorker

allYours            :: PacketHeader
allYours            = PacketHeader AllYours Req DomainWorker

workExceptionWorker :: PacketHeader
workExceptionWorker = PacketHeader WorkException Req DomainWorker

workExceptionClient :: PacketHeader
workExceptionClient = PacketHeader WorkException Res DomainClient

optionReq           :: PacketHeader
optionReq           = PacketHeader OptionReq Req DomainBoth

optionRes           :: PacketHeader
optionRes           = PacketHeader OptionRes Res DomainBoth

workDataWorker      :: PacketHeader
workDataWorker      = PacketHeader WorkData Req DomainWorker

workDataClient      :: PacketHeader
workDataClient      = PacketHeader WorkData Res DomainClient

workWarningWorker   :: PacketHeader
workWarningWorker   = PacketHeader WorkWarning Req DomainWorker

workWarningClient   :: PacketHeader
workWarningClient   = PacketHeader WorkWarning Res DomainClient

grabJobUniq         :: PacketHeader
grabJobUniq         = PacketHeader GrabJobUniq Req DomainWorker

jobAssignUniq       :: PacketHeader
jobAssignUniq       = PacketHeader JobAssignUniq Res DomainWorker

submitJobHighBg     :: PacketHeader
submitJobHighBg     = PacketHeader SubmitJobHighBg Req DomainClient

submitJobLow        :: PacketHeader
submitJobLow        = PacketHeader SubmitJobLow Req DomainClient

submitJobLowBg      :: PacketHeader
submitJobLowBg      = PacketHeader SubmitJobLowBg Req DomainClient

submitJobSched      :: PacketHeader
submitJobSched      = PacketHeader SubmitJobSched Req DomainClient

submitJobEpoch      :: PacketHeader
submitJobEpoch      = PacketHeader SubmitJobEpoch Req DomainClient

-- | marshalWord32 returns the ByteString representation of
--   the supplied Int as a big-endian Word32.
marshalWord32       :: Int -> L.ByteString
marshalWord32 n     = runPut $ putWord32be $ fromIntegral n

-- |Encode the 'magic' part of the packet header.
renderMagic :: PacketMagic -> L.ByteString
renderMagic Req = "\0REQ"
renderMagic Res = "\0RES"
renderMagic UnknownMagic = ""

-- |Takes four bytes and returns the magic type. This is the first
-- component of a Gearman packet.
parseMagic :: L.ByteString -> PacketMagic
parseMagic m = case m of
    "\0REQ" -> Req
    "\0RES" -> Res
    _       -> UnknownMagic

-- |Takes four bytes and returns a packet type.
parsePacketType :: L.ByteString -> Either GearmanError PacketHeader
parsePacketType d = case fromWord32 $ runGet getWord32be d of
    Left err -> Left err
    Right x  -> case x of
        EchoRes       -> Right echoRes
        Error         -> Right error
        Noop          -> Right noop
        NoJob         -> Right noJob
        JobAssign     -> Right jobAssign
        JobAssignUniq -> Right jobAssignUniq
        JobCreated    -> Right jobCreated
        WorkComplete  -> Right workCompleteWorker

        word          -> Left $ "unexpected " ++ show word

-- |Takes a 4-bytestring and returns the big-endian word32
-- representation.
parseDataSize :: L.ByteString -> Int
parseDataSize = fromEnum . runGet getWord32be

-- |Given a domain (either ClientDomain or WorkerDomain, depending on
-- what's calling it) and four ByteStrings representing the four
-- components of a packet (magic, type, data size and data), return an
-- unmarshalled packet (or an error if the data is invalid).
parsePacket :: PacketDomain ->
               L.ByteString ->
               L.ByteString ->
               L.ByteString ->
               L.ByteString ->
               Either GearmanError GearmanPacket
parsePacket domain magic typ dataSize args =
    case parseMagic magic of
        UnknownMagic -> Left $ "invalid packet magic" ++ prettyHex (L.toStrict magic)
        magic' -> case parsePacketType typ of
            Left err -> Left err
            Right typ' -> Right $ GearmanPacket typ'
                                                (parseDataSize dataSize)
                                                (unpackData args)

-- |Return the ByteString representation of a PacketHeader.
renderHeader :: PacketHeader -> L.ByteString
renderHeader PacketHeader{..} =
    runPut $ do
        putLazyByteString (renderMagic magic)
        (putWord32be . fromIntegral . fromEnum) packetType

-- | packData takes a list of message parts (ByteStrings) and concatenates
--   them with null bytes in between and the length in front.
packData    :: [L.ByteString] -> L.ByteString
packData [] = runPut $ putWord32be 0
packData d  = runPut $ do
    putWord32be $ fromIntegral $ L.length (toPayload d)
    putLazyByteString $ toPayload d
  where
    toPayload = L.intercalate "\0"

-- | unpackData takes the data segment of a Gearman message (after the size
-- word) and returns a list of the message arguments.
unpackData    :: L.ByteString -> [L.ByteString]
unpackData "" = []
unpackData d  = (L.split . fromIntegral . fromEnum) '\0' d

-- |Construct an ECHO_REQ packet.
buildEchoReq        :: [L.ByteString] -> L.ByteString
buildEchoReq        = L.append (renderHeader echoReq) . packData

-- |Construct a CAN_DO request packet (workers send these to register
-- functions with the server).
buildCanDoReq       :: L.ByteString -> L.ByteString
buildCanDoReq       = L.append (renderHeader canDo) . packData . (:[])

-- |Construct a CAN_DO_TIMEOUT packet (as above, but with a timeout value).
-- FIXME: confirm timestamp format.
buildCanDoTimeoutReq ::
    L.ByteString ->
    Int ->
    L.ByteString
buildCanDoTimeoutReq fn t = L.append (renderHeader canDoTimeout) (packData [fn, marshalWord32 t])

-- |Construct a WORK_COMPLETE packet (sent by workers when they
-- finish a job).
buildWorkCompleteReq :: J.JobHandle -> J.JobData -> L.ByteString
buildWorkCompleteReq handle response =
    L.append (renderHeader workCompleteWorker) $ packData [handle, response]

-- |Construct a WORK_FAIL packet (sent by workers to report a job
-- failure with no accompanying data).
buildWorkFailReq :: J.JobHandle -> L.ByteString
buildWorkFailReq = L.append (renderHeader workFailWorker) . packData . (:[])

-- |Construct a WORK_EXCEPTION packet (sent by workers to report a job
-- failure with accompanying exception data).
buildWorkExceptionReq :: J.JobHandle -> L.ByteString -> L.ByteString
buildWorkExceptionReq handle msg = L.append (renderHeader workFailWorker) $ packData [handle, msg]

-- |Construct a WORK_DATA packet (sent by workers when they have
-- intermediate data to send for a job).
buildWorkDataReq :: J.JobHandle -> J.JobData -> L.ByteString
buildWorkDataReq handle payload =
    L.append (renderHeader workDataWorker) $ packData [handle, payload]

-- |Construct a WORK_WARNING packet (same as above, but treated as a
-- warning).
buildWorkWarningReq :: J.JobHandle -> J.JobData -> L.ByteString
buildWorkWarningReq handle payload =
    L.append (renderHeader workWarningWorker) $ packData [handle, payload]

-- |Construct a WORK_STATUS packet (sent by workers to inform the server
-- of the percentage of the job that has been completed).
buildWorkStatusReq :: J.JobHandle -> J.JobStatus -> L.ByteString
buildWorkStatusReq handle status =
    L.append (renderHeader workStatusWorker) $ packData args
  where
    args = handle : map marshalWord32 (toList status)
    toList t = [fst t, snd t]

-- |Construct a GRAB_JOB packet (sent by workers to inform the server
-- it's ready for a new job).
buildGrabJobReq :: L.ByteString
buildGrabJobReq = L.append (renderHeader grabJob) (packData [])

-- |Construct a GRAB_JOB_UNIQ packet (sent by workers to inform the server
-- it's ready for a new job and it wants a unique client ID).
buildGrabJobUniqReq :: L.ByteString
buildGrabJobUniqReq = L.append (renderHeader grabJobUniq) (packData [])

-- |Construct a PRE_SLEEP packet (sent by workers to inform the server
-- they are going to sleep).
buildPreSleepReq :: L.ByteString
buildPreSleepReq = L.append (renderHeader preSleep) (packData [])

-- |Construct a NOOP packet (sent by the server to wake up a sleeping
-- worker).
buildNoopRes :: L.ByteString
buildNoopRes = L.append (renderHeader noop) (packData [])

-- | Construct a SUBMIT_JOB packet (sent by the client to request
-- work to be done.
buildSubmitJob :: L.ByteString -> L.ByteString -> L.ByteString -> L.ByteString
buildSubmitJob funcName uniqId workload =
    L.append (renderHeader submitJob) (packData [funcName, uniqId, workload])

