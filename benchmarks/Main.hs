module Main where

import qualified Data.Vector as F
import Gauge
import Gauge.Main
import qualified Hasql.Connection as A
import qualified Hasql.Decoders as D
import qualified Hasql.Encoders as E
import qualified Hasql.Session as B
import qualified Hasql.Statement as C
import Prelude

main =
  do
    Right connection <- acquireConnection
    useConnection connection
  where
    acquireConnection =
      A.acquire ""
    useConnection connection =
      defaultMain
        [ sessionBench "largeResultInVector" sessionWithSingleLargeResultInVector,
          sessionBench "largeResultInList" sessionWithSingleLargeResultInList,
          sessionBench "manyLargeResults" sessionWithManyLargeResults,
          sessionBench "manySmallResults" sessionWithManySmallResults
        ]
      where
        sessionBench :: NFData a => String -> B.Session a -> Benchmark
        sessionBench name session =
          bench name (nfIO (fmap (either (error "") id) (B.run session connection)))

-- * Sessions

sessionWithManySmallParameters :: Vector (Int64, Int64) -> B.Session ()
sessionWithManySmallParameters =
  error "TODO: sessionWithManySmallParameters"

sessionWithSingleLargeResultInVector :: B.Session (Vector (Int64, Int64))
sessionWithSingleLargeResultInVector =
  B.statement () statementWithManyRowsInVector

sessionWithManyLargeResults :: B.Session [Vector (Int64, Int64)]
sessionWithManyLargeResults =
  replicateM 1000 (B.statement () statementWithManyRowsInVector)

sessionWithSingleLargeResultInList :: B.Session [(Int64, Int64)]
sessionWithSingleLargeResultInList =
  B.statement () statementWithManyRowsInList

sessionWithManySmallResults :: B.Session [(Int64, Int64)]
sessionWithManySmallResults =
  replicateM 1000 (B.statement () statementWithSingleRow)

-- * Statements

statementWithManyParameters :: C.Statement (Vector (Int64, Int64)) ()
statementWithManyParameters =
  error "TODO: statementWithManyParameters"

statementWithSingleRow :: C.Statement () (Int64, Int64)
statementWithSingleRow =
  C.Statement template encoder decoder True
  where
    template =
      "SELECT 1, 2"
    encoder =
      conquer
    decoder =
      D.singleRow row
      where
        row =
          tuple <$> (D.column . D.nonNullable) D.int8 <*> (D.column . D.nonNullable) D.int8
          where
            tuple !a !b =
              (a, b)

statementWithManyRows :: (D.Row (Int64, Int64) -> D.Result result) -> C.Statement () result
statementWithManyRows decoder =
  C.Statement template encoder (decoder rowDecoder) True
  where
    template =
      "SELECT generate_series(0,1000) as a, generate_series(1000,2000) as b"
    encoder =
      conquer
    rowDecoder =
      tuple <$> (D.column . D.nonNullable) D.int8 <*> (D.column . D.nonNullable) D.int8
      where
        tuple !a !b =
          (a, b)

statementWithManyRowsInVector :: C.Statement () (Vector (Int64, Int64))
statementWithManyRowsInVector =
  statementWithManyRows D.rowVector

statementWithManyRowsInList :: C.Statement () [(Int64, Int64)]
statementWithManyRowsInList =
  statementWithManyRows D.rowList
