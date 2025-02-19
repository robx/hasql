module Hasql.Private.Decoders.Row where

import qualified Database.PostgreSQL.LibPQ as LibPQ
import qualified Hasql.Private.Decoders.Value as Value
import Hasql.Private.Errors
import Hasql.Private.Prelude hiding (error)
import qualified PostgreSQL.Binary.Decoding as A

newtype Row a
  = Row (ReaderT Env (ExceptT RowError IO) a)
  deriving (Functor, Applicative, Monad)

instance MonadFail Row where
  fail = error . ValueError . fromString

data Env
  = Env !LibPQ.Result !LibPQ.Row !LibPQ.Column !Bool !(IORef LibPQ.Column)

-- * Functions

{-# INLINE run #-}
run :: Row a -> (LibPQ.Result, LibPQ.Row, LibPQ.Column, Bool) -> IO (Either (Int, RowError) a)
run (Row impl) (result, row, columnsAmount, integerDatetimes) =
  do
    columnRef <- newIORef 0
    runExceptT (runReaderT impl (Env result row columnsAmount integerDatetimes columnRef)) >>= \case
      Left e -> do
        LibPQ.Col col <- readIORef columnRef
        -- -1 because succ is applied before the error is returned
        pure $ Left (fromIntegral col - 1, e)
      Right x -> pure $ Right x

{-# INLINE error #-}
error :: RowError -> Row a
error x =
  Row (ReaderT (const (ExceptT (pure (Left x)))))

-- |
-- Next value, decoded using the provided value decoder.
{-# INLINE value #-}
value :: Value.Value a -> Row (Maybe a)
value valueDec =
  {-# SCC "value" #-}
  Row $
    ReaderT $ \(Env result row columnsAmount integerDatetimes columnRef) -> ExceptT $ do
      col <- readIORef columnRef
      writeIORef columnRef (succ col)
      if col < columnsAmount
        then do
          valueMaybe <- {-# SCC "getvalue'" #-} LibPQ.getvalue' result row col
          pure $
            case valueMaybe of
              Nothing ->
                Right Nothing
              Just value ->
                fmap Just $
                  mapLeft ValueError $
                    {-# SCC "decode" #-} A.valueParser (Value.run valueDec integerDatetimes) value
        else pure (Left EndOfInput)

-- |
-- Next value, decoded using the provided value decoder.
{-# INLINE nonNullValue #-}
nonNullValue :: Value.Value a -> Row a
nonNullValue valueDec =
  {-# SCC "nonNullValue" #-}
  value valueDec >>= maybe (error UnexpectedNull) pure
