module Hasql.Private.Session where

import qualified Database.PostgreSQL.LibPQ as LibPQ
import qualified Hasql.Private.Connection as Connection
import qualified Hasql.Private.Decoders as Decoders
import qualified Hasql.Private.Decoders.Result as Decoders.Result
import qualified Hasql.Private.Decoders.Results as Decoders.Results
import qualified Hasql.Private.Encoders as Encoders
import qualified Hasql.Private.Encoders.Params as Encoders.Params
import Hasql.Private.Errors
import qualified Hasql.Private.IO as IO
import Hasql.Private.Prelude
import qualified Hasql.Private.Settings as Settings
import qualified Hasql.Statement as Statement

-- |
-- A batch of actions to be executed in the context of a database connection.
newtype Session a
  = Session (ReaderT Connection.Connection (ExceptT QueryError IO) a)
  deriving (Functor, Applicative, Monad, MonadError QueryError, MonadIO, MonadReader Connection.Connection)

-- |
-- Executes a bunch of commands on the provided connection.
run :: Session a -> Connection.Connection -> IO (Either QueryError a)
run (Session impl) connection =
  runExceptT $
    runReaderT impl connection

-- |
-- Possibly a multi-statement query,
-- which however cannot be parameterized or prepared,
-- nor can any results of it be collected.
sql :: ByteString -> Session ()
sql sql =
  Session $
    ReaderT $ \(Connection.Connection pqConnectionRef integerDatetimes registry) ->
      ExceptT $
        fmap (mapLeft (QueryError sql [])) $
          withMVar pqConnectionRef $ \pqConnection -> do
            r1 <- IO.sendNonparametricStatement pqConnection sql
            r2 <- IO.getResults pqConnection integerDatetimes decoder
            return $ r1 *> r2
  where
    decoder =
      Decoders.Results.single Decoders.Result.noResult

-- |
-- Parameters and a specification of a parametric single-statement query to apply them to.
statement :: params -> Statement.Statement params result -> Session result
statement input (Statement.Statement template (Encoders.Params paramsEncoder) decoder preparable) =
  Session $
    ReaderT $ \(Connection.Connection pqConnectionRef integerDatetimes registry) ->
      ExceptT $
        fmap (mapLeft (QueryError template inputReps)) $
          withMVar pqConnectionRef $ \pqConnection -> do
            r1 <- IO.sendParametricStatement pqConnection integerDatetimes registry template paramsEncoder preparable input
            r2 <- IO.getResults pqConnection integerDatetimes (unsafeCoerce decoder)
            return $ r1 *> r2
  where
    inputReps =
      let Encoders.Params.Params (Op encoderOp) = paramsEncoder
          step (_, _, _, rendering) acc =
            rendering : acc
       in foldr step [] (encoderOp input)

lg = putStrLn

prepareStatement :: params -> Statement.Statement params () -> Session ()
prepareStatement input (Statement.Statement template (Encoders.Params paramsEncoder) _ preparable) =
  Session $
    ReaderT $ \(Connection.Connection pqConnectionRef integerDatetimes registry) ->
      ExceptT $
        fmap (mapLeft (QueryError template inputReps)) $
          withMVar pqConnectionRef $ \pqConnection -> do
            IO.prepareParametricStatement pqConnection integerDatetimes registry template paramsEncoder preparable input
  where
    inputReps =
      let Encoders.Params.Params (Op encoderOp) = paramsEncoder
          step (_, _, _, rendering) acc =
            rendering : acc
       in foldr step [] (encoderOp input)

queuePipelineStatement :: params -> Statement.Statement params () -> Session ()
queuePipelineStatement input (Statement.Statement template (Encoders.Params paramsEncoder) _ preparable) =
  Session $
    ReaderT $ \(Connection.Connection pqConnectionRef integerDatetimes registry) ->
      ExceptT $
        fmap (mapLeft (QueryError template inputReps)) $
          withMVar pqConnectionRef $ \pqConnection -> do
            IO.ensureNonblocking pqConnection
            lg "ensuring pipeline mode"
            IO.ensurePipelineMode pqConnection
            lg "ensured pipeline mode"
            r <- IO.sendParametricStatement pqConnection integerDatetimes registry template paramsEncoder preparable input
            lg "sent statement"
            return r
  where
    inputReps =
      let Encoders.Params.Params (Op encoderOp) = paramsEncoder
          step (_, _, _, rendering) acc =
            rendering : acc
       in foldr step [] (encoderOp input)

finalPipelineStatement :: Int -> params -> Statement.Statement params result -> Session result
finalPipelineStatement numQueued input (Statement.Statement template (Encoders.Params paramsEncoder) decoder preparable) =
  Session $
    ReaderT $ \(Connection.Connection pqConnectionRef integerDatetimes registry) ->
      ExceptT $
        fmap (mapLeft (QueryError template inputReps)) $
          withMVar pqConnectionRef $ \pqConnection -> do
            lg "sending final statement"
            r1 <- IO.sendParametricStatement pqConnection integerDatetimes registry template paramsEncoder preparable input
            lg "syncing pipeline"
            IO.syncPipeline pqConnection
            lg $ "collecting " <> show numQueued <> " results"
            forM_ [1..numQueued] $ \_ ->
              IO.getResults pqConnection integerDatetimes (unsafeCoerce Decoders.noResult)
            lg $ "getting actual result"
            r2 <- IO.getResults pqConnection integerDatetimes (unsafeCoerce decoder)
            lg $ "getting sync result"
            r3 <- IO.getSyncPipelineResult pqConnection
            lg $ "disabling pipeline mode"
            IO.exitPipelineMode pqConnection
            return $ r1 *> r2 <* r3
  where
    inputReps =
      let Encoders.Params.Params (Op encoderOp) = paramsEncoder
          step (_, _, _, rendering) acc =
            rendering : acc
       in foldr step [] (encoderOp input)
