module Main where

import Prelude
import qualified Hasql.Connection
import qualified Hasql.Statement
import qualified Hasql.Encoders
import qualified Hasql.Decoders
import qualified Hasql.Session
import qualified Main.Statements as Statements


main =
  acquire >>= use
  where
    acquire =
      (,) <$> acquire <*> acquire
      where
        acquire =
          join $
          fmap (either (fail . show) return) $
          Hasql.Connection.acquire connectionSettings
          where
            connectionSettings =
              Hasql.Connection.settings "localhost" 5432 "rob" "" "postgres"
    use (connection1, connection2) =
      do
        beginVar <- newEmptyMVar
        finishVar <- newEmptyMVar
        forkIO $ do
          traceM "1: in"
          putMVar beginVar ()
          session connection1 (Hasql.Session.statement 0.2 Statements.selectSleep)
          traceM "1: out"
          void (tryPutMVar finishVar False)
        forkIO $ do
          takeMVar beginVar
          traceM "2: in"
          session connection2 (Hasql.Session.statement 0.1 Statements.selectSleep)
          traceM "2: out"
          void (tryPutMVar finishVar True)
        bool exitFailure exitSuccess . traceShowId =<< takeMVar finishVar
      where
        session connection session =
          Hasql.Session.run session connection >>=
          either (fail . show) return
