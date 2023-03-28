-- |
-- An API of low-level IO operations.
module Hasql.Private.IO.Pipeline where

import qualified Database.PostgreSQL.LibPQ as LibPQ
import qualified Hasql.Private.Commands as Commands
import qualified Hasql.Private.Decoders as Decoders
import qualified Hasql.Private.Decoders.Result as ResultDecoders
import qualified Hasql.Private.Decoders.Results as ResultsDecoders
import qualified Hasql.Private.Encoders.Params as ParamsEncoders
import Hasql.Private.Errors
import Hasql.Private.Prelude

data PConnection
  = PConnection { pcConn :: !LibPQ.Connection, pcOutstanding :: !(IORef Int) }

{-# INLINE bumpPipeline #-}
bumpPipeline :: PConnection -> IO ()
bumpPipeline (PConnection _ outstanding) = modifyIORef outstanding (+1)

{-# INLINE newNullConnection #-}
newNullConnection :: IO PConnection
newNullConnection = PConnection <$> LibPQ.newNullConnection <*> newIORef 0

{-# INLINE acquireConnection #-}
acquireConnection :: ByteString -> IO PConnection
acquireConnection s = PConnection <$> LibPQ.connectdb s <*> newIORef 0

{-# INLINE releaseConnection #-}
releaseConnection :: PConnection -> IO ()
releaseConnection (PConnection connection _) =
  -- could (should?) collect outstanding pipelined results here
  LibPQ.finish connection

{-# INLINE initConnection #-}
initConnection :: PConnection -> IO ()
initConnection (PConnection conn _) = do
  void $ LibPQ.exec conn (Commands.asBytes (Commands.setEncodersToUTF8 <> Commands.setMinClientMessagesToWarning))
  void $ LibPQ.setnonblocking conn True
  void $ LibPQ.enterPipelineMode conn

{-# INLINE getResults #-}
getResults :: PConnection -> Bool -> ResultsDecoders.Results a -> IO (Either CommandError a)
getResults (PConnection pqConnection pipeline) integerDatetimes decoder = do
  void $ LibPQ.pipelineSync pqConnection
  nOutstanding <- readIORef pipeline
  rs <- forM [2..nOutstanding] $ \_ -> do
    ra <- ResultsDecoders.run (unsafeCoerce Decoders.noResult) (integerDatetimes, pqConnection)
    rb <- dropRemainders
    return $ ra <* rb
  let r0 = foldl (<*) (return ()) rs

  r1 <- get
  r2 <- dropRemainders

  r3 <- ResultsDecoders.run (ResultsDecoders.single ResultDecoders.pipelineSync) (integerDatetimes, pqConnection)
  writeIORef pipeline 0

  return $ r0 *> r1 <* r2 <* r3
  where
    get =
      ResultsDecoders.run decoder (integerDatetimes, pqConnection)
    dropRemainders =
      ResultsDecoders.run ResultsDecoders.dropRemainders (integerDatetimes, pqConnection)

sendPrepare connection key template oids = do
  bumpPipeline connection
  LibPQ.sendPrepare (pcConn connection) key template oids

{-# INLINE checkedSend #-}
checkedSend :: PConnection -> IO Bool -> IO (Either CommandError ())
checkedSend connection send = do
  bumpPipeline connection
  send >>= \case
    False -> fmap (Left . ClientError) $ LibPQ.errorMessage (pcConn connection)
    True -> pure (Right ())
