-- |
-- An API of low-level IO operations.
module Hasql.Private.IO.Sync where

import qualified Database.PostgreSQL.LibPQ as LibPQ
import qualified Hasql.Private.Commands as Commands
import qualified Hasql.Private.Decoders as Decoders
import qualified Hasql.Private.Decoders.Result as ResultDecoders
import qualified Hasql.Private.Decoders.Results as ResultsDecoders
import qualified Hasql.Private.Encoders.Params as ParamsEncoders
import Hasql.Private.Errors
import Hasql.Private.Prelude

type PConnection = LibPQ.Connection

pcConn :: PConnection -> LibPQ.Connection
pcConn = id

sendQuery :: LibPQ.Connection -> ByteString -> IO Bool
sendQuery = LibPQ.sendQuery

{-# INLINE newNullConnection #-}
newNullConnection :: IO PConnection
newNullConnection = LibPQ.newNullConnection

{-# INLINE acquireConnection #-}
acquireConnection :: ByteString -> IO PConnection
acquireConnection = LibPQ.connectdb

{-# INLINE releaseConnection #-}
releaseConnection :: PConnection -> IO ()
releaseConnection = LibPQ.finish

{-# INLINE initConnection #-}
initConnection :: PConnection -> IO ()
initConnection c =
  void $ LibPQ.exec c (Commands.asBytes (Commands.setEncodersToUTF8 <> Commands.setMinClientMessagesToWarning))

{-# INLINE getResults #-}
getResults :: PConnection -> Bool -> ResultsDecoders.Results a -> IO (Either CommandError a)
getResults pqConnection integerDatetimes decoder = do
  r1 <- get
  r2 <- dropRemainders
  return $ r1 <* r2
  where
    get =
      ResultsDecoders.run decoder (integerDatetimes, pqConnection)
    dropRemainders =
      ResultsDecoders.run ResultsDecoders.dropRemainders (integerDatetimes, pqConnection)

sendPrepare = LibPQ.sendPrepare

{-# INLINE checkedSend #-}
checkedSend :: PConnection -> IO Bool -> IO (Either CommandError ())
checkedSend connection send = do
  send >>= \case
    False -> fmap (Left . ClientError) $ LibPQ.errorMessage connection
    True -> pure (Right ())
