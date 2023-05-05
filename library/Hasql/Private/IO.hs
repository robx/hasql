-- |
-- An API of low-level IO operations.
module Hasql.Private.IO
  ( PConnection
  , acquireConnection
  , releaseConnection
  , newNullConnection
  , checkConnectionStatus
  , acquirePreparedStatementRegistry
  , initConnection
  , pcConn
  , getIntegerDatetimes
  , getResults
  , sendParametricStatement
  , sendNonparametricStatement
  )
where

import qualified Data.DList as DList
import qualified Database.PostgreSQL.LibPQ as LibPQ
import qualified Hasql.Private.Commands as Commands
import qualified Hasql.Private.Decoders as Decoders
import qualified Hasql.Private.Decoders.Result as ResultDecoders
import qualified Hasql.Private.Decoders.Results as ResultsDecoders
import qualified Hasql.Private.Encoders.Params as ParamsEncoders
import Hasql.Private.Errors
import Hasql.Private.Prelude
import qualified Hasql.Private.PreparedStatementRegistry as PreparedStatementRegistry
import Hasql.Private.IO.Pipeline
--import Hasql.Private.IO.Sync

{-# INLINE acquirePreparedStatementRegistry #-}
acquirePreparedStatementRegistry :: IO PreparedStatementRegistry.PreparedStatementRegistry
acquirePreparedStatementRegistry =
  PreparedStatementRegistry.new

{-# INLINE checkConnectionStatus #-}
checkConnectionStatus :: PConnection -> IO (Maybe (Maybe ByteString))
checkConnectionStatus conn =
  do
    s <- LibPQ.status $ pcConn conn
    case s of
      LibPQ.ConnectionOk -> return Nothing
      _ -> fmap Just (LibPQ.errorMessage $ pcConn conn)

{-# INLINE checkServerVersion #-}
checkServerVersion :: LibPQ.Connection -> IO (Maybe Int)
checkServerVersion c =
  fmap (mfilter (< 80200) . Just) (LibPQ.serverVersion c)

{-# INLINE getIntegerDatetimes #-}
getIntegerDatetimes :: PConnection -> IO Bool
getIntegerDatetimes conn =
  fmap decodeValue $ LibPQ.parameterStatus (pcConn conn) "integer_datetimes"
  where
    decodeValue =
      \case
        Just "on" -> True
        _ -> False

{-# INLINE getPreparedStatementKey #-}
getPreparedStatementKey ::
  PConnection ->
  PreparedStatementRegistry.PreparedStatementRegistry ->
  ByteString ->
  [LibPQ.Oid] ->
  IO (Either CommandError ByteString)
getPreparedStatementKey connection registry template oidList =
  {-# SCC "getPreparedStatementKey" #-}
  PreparedStatementRegistry.update localKey onNewRemoteKey onOldRemoteKey registry
  where
    localKey =
      PreparedStatementRegistry.LocalKey template wordOIDList
      where
        wordOIDList =
          map (\(LibPQ.Oid x) -> fromIntegral x) oidList
    onNewRemoteKey key =
      do
        sent <- sendPrepare connection key template (mfilter (not . null) (Just oidList))
        let resultsDecoder =
              if sent
                then ResultsDecoders.single ResultDecoders.noResult
                else ResultsDecoders.clientError
        fmap resultsMapping $ getResults connection undefined resultsDecoder
      where
        resultsMapping =
          \case
            Left x -> (False, Left x)
            Right _ -> (True, Right key)
    onOldRemoteKey key =
      pure (pure key)

{-# INLINE sendPreparedParametricStatement #-}
sendPreparedParametricStatement ::
  PConnection ->
  PreparedStatementRegistry.PreparedStatementRegistry ->
  Bool ->
  ByteString ->
  ParamsEncoders.Params a ->
  a ->
  IO (Either CommandError ())
sendPreparedParametricStatement connection registry integerDatetimes template (ParamsEncoders.Params (Op encoderOp)) input =
  let (oidList, valueAndFormatList) =
        let step (oid, format, encoder, _) ~(oidList, bytesAndFormatList) =
              (,)
                (oid : oidList)
                (fmap (\bytes -> (bytes, format)) (encoder integerDatetimes) : bytesAndFormatList)
         in foldr step ([], []) (encoderOp input)
   in runExceptT $ do
        key <- ExceptT $ getPreparedStatementKey connection registry template oidList
        ExceptT $ checkedSend connection $ LibPQ.sendQueryPrepared (pcConn connection) key valueAndFormatList LibPQ.Binary

{-# INLINE sendUnpreparedParametricStatement #-}
sendUnpreparedParametricStatement ::
  PConnection ->
  Bool ->
  ByteString ->
  ParamsEncoders.Params a ->
  a ->
  IO (Either CommandError ())
sendUnpreparedParametricStatement connection integerDatetimes template (ParamsEncoders.Params (Op encoderOp)) input =
  let params =
        let step (oid, format, encoder, _) acc =
              ((,,) <$> pure oid <*> encoder integerDatetimes <*> pure format) : acc
         in foldr step [] (encoderOp input)
   in checkedSend connection $ LibPQ.sendQueryParams (pcConn connection) template params LibPQ.Binary

{-# INLINE sendParametricStatement #-}
sendParametricStatement ::
  PConnection ->
  Bool ->
  PreparedStatementRegistry.PreparedStatementRegistry ->
  ByteString ->
  ParamsEncoders.Params a ->
  Bool ->
  a ->
  IO (Either CommandError ())
sendParametricStatement connection integerDatetimes registry template encoder prepared params =
  {-# SCC "sendParametricStatement" #-}
  if prepared
    then sendPreparedParametricStatement connection registry integerDatetimes template encoder params
    else sendUnpreparedParametricStatement connection integerDatetimes template encoder params

{-# INLINE sendNonparametricStatement #-}
sendNonparametricStatement :: PConnection -> ByteString -> IO (Either CommandError ())
sendNonparametricStatement connection sql =
  checkedSend connection $ sendQuery (pcConn connection) sql
