-- |
-- An API of low-level IO operations.
module Hasql.Private.IO where

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

data Pipeline = PipelineOff | PipelineQueued Int

data PConnection
  = PConnection { pcConn :: !LibPQ.Connection, pcOutstanding :: !(IORef Pipeline) }

{-# INLINE startPipeline #-}
startPipeline :: PConnection -> IO ()
startPipeline (PConnection conn outstanding) = do
  lg "startPipeline"
  rb <- LibPQ.setnonblocking conn True
  lg $ "set nonblocking: " <> show rb
  r <- LibPQ.enterPipelineMode conn -- fixme check result
  lg $ "startPipeline success: " <> show r
  modifyIORef outstanding
    (\p -> case p of PipelineOff -> PipelineQueued 0
                     _           -> p)

{-# INLINE bumpPipeline #-}
bumpPipeline :: PConnection -> IO ()
bumpPipeline (PConnection _ outstanding) = do
  lg "bumpPipeline"
  modifyIORef outstanding
    (\p -> case p of PipelineOff -> PipelineOff
                     PipelineQueued n -> PipelineQueued $ n + 1)

stopPipeline :: PConnection -> IO ()
stopPipeline (PConnection conn pipeline) = do
  lg "stopPipeline"
  p <- readIORef pipeline
  case p of
    PipelineOff -> do
      lg "already off!"
    PipelineQueued n -> do
      lg $ "outstanding (should be 0): " <> show n
  writeIORef pipeline PipelineOff
  exitPipelineMode conn
  rb <- LibPQ.setnonblocking conn False
  lg $ "unset nonblocking: " <> show rb

{-# INLINE newNullConnection #-}
newNullConnection :: IO PConnection
newNullConnection = PConnection <$> LibPQ.newNullConnection <*> newIORef PipelineOff

{-# INLINE acquireConnection #-}
acquireConnection :: ByteString -> IO PConnection
acquireConnection s = PConnection <$> LibPQ.connectdb s <*> newIORef PipelineOff

{-# INLINE acquirePreparedStatementRegistry #-}
acquirePreparedStatementRegistry :: IO PreparedStatementRegistry.PreparedStatementRegistry
acquirePreparedStatementRegistry =
  PreparedStatementRegistry.new

{-# INLINE releaseConnection #-}
releaseConnection :: PConnection -> IO ()
releaseConnection (PConnection connection _) =
  LibPQ.finish connection

{-# INLINE checkConnectionStatus #-}
checkConnectionStatus :: PConnection -> IO (Maybe (Maybe ByteString))
checkConnectionStatus (PConnection c _) =
  do
    s <- LibPQ.status c
    case s of
      LibPQ.ConnectionOk -> return Nothing
      _ -> fmap Just (LibPQ.errorMessage c)

exitPipelineMode :: LibPQ.Connection -> IO ()
exitPipelineMode = void . LibPQ.exitPipelineMode

syncPipeline :: LibPQ.Connection -> IO ()
syncPipeline c = do
  lg "syncing pipeline"
  r <- LibPQ.pipelineSync c
  lg $ "success: " <> show r

{-# INLINE checkServerVersion #-}
checkServerVersion :: LibPQ.Connection -> IO (Maybe Int)
checkServerVersion c =
  fmap (mfilter (< 80200) . Just) (LibPQ.serverVersion c)

{-# INLINE getIntegerDatetimes #-}
getIntegerDatetimes :: PConnection -> IO Bool
getIntegerDatetimes (PConnection c _) =
  fmap decodeValue $ LibPQ.parameterStatus c "integer_datetimes"
  where
    decodeValue =
      \case
        Just "on" -> True
        _ -> False

{-# INLINE initConnection #-}
initConnection :: PConnection -> IO ()
initConnection (PConnection c _) =
  void $ LibPQ.exec c (Commands.asBytes (Commands.setEncodersToUTF8 <> Commands.setMinClientMessagesToWarning))


--lg = putStrLn
--lgl r = case r of Left e -> lg $ "error: " <> show e
--                  _ -> lg "success"


lg = const $ pure ()
lgl = const $ pure ()

{-# INLINE getResults #-}
getResults :: PConnection -> Bool -> ResultsDecoders.Results a -> IO (Either CommandError a)
getResults (PConnection pqConnection pipeline) integerDatetimes decoder = do
  p <- readIORef pipeline
  case p of
    PipelineQueued nOutstanding -> do
      syncPipeline pqConnection
      lg $ "collecting " <> show nOutstanding <> " queued results"
      forM_ [2..nOutstanding] $ \_ -> do
         lg "getting queued result"
         r <- ResultsDecoders.run (unsafeCoerce Decoders.noResult) (integerDatetimes, pqConnection)
         lgl r
         r2 <- dropRemainders
         lgl r2
    PipelineOff -> return ()

  lg "getting actual result"
  r1 <- get
  lgl r1
  r2 <- dropRemainders
  lgl r2
  case p of
    PipelineQueued _ -> do
      lg $ "getting sync result"
      r3 <- ResultsDecoders.run (ResultsDecoders.single ResultDecoders.pipelineSync) (integerDatetimes, pqConnection)
      lg $ show r3
      writeIORef pipeline $ PipelineQueued 0
    _ -> return ()
  return $ r1 <* r2
  where
    get =
      ResultsDecoders.run decoder (integerDatetimes, pqConnection)
    dropRemainders =
      ResultsDecoders.run ResultsDecoders.dropRemainders (integerDatetimes, pqConnection)

sendPrepare connection key template oids = do
  lg "sendPrepare"
  bumpPipeline connection
  LibPQ.sendPrepare (pcConn connection) key template oids

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

{-# INLINE checkedSend #-}
checkedSend :: PConnection -> IO Bool -> IO (Either CommandError ())
checkedSend connection send = do
  bumpPipeline connection
  send >>= \case
    False -> fmap (Left . ClientError) $ LibPQ.errorMessage (pcConn connection)
    True -> pure (Right ())

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
  checkedSend connection $ LibPQ.sendQuery (pcConn connection) sql
