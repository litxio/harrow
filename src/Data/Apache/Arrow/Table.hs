{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}

module Data.Apache.Arrow.Table where

import qualified GI.Arrow as G
import System.IO.Unsafe
import Data.Int
import Data.Word
import Data.Text (Text)
import Control.Exception hiding (IndexOutOfBounds)
import Data.GI.Base.Utils (allocMem)
import Data.GI.Base (managedForeignPtr)
import Control.Monad.IO.Class
import GHC.Stack
import qualified Data.Vector.Storable as SV
import Data.Coerce

import Foreign.Ptr
import Foreign.ForeignPtr
import Foreign.Storable (peek, Storable)
import Data.Apache.Arrow.Types
import Data.Apache.Arrow.Instances


boxChunkedArray :: G.ChunkedArray -> ChunkedArray a
boxChunkedArray arr = unsafePerformIO $ do
  let _gChunkedArray = arr
  _chunkedArrayLength <- fromIntegral <$> G.chunkedArrayGetLength _gChunkedArray
  _chunkedArrayNumChunks <- fromIntegral <$> G.chunkedArrayGetNChunks _gChunkedArray
  return ChunkedArray{..}


getColumn :: forall a. ArrowType a => Int -> Table -> Either ArrowError (Column a)
getColumn i table 
  | i < 0 = Left (IndexOutOfBounds i)
  | i >= _tableNumColumns table = Left (IndexOutOfBounds i)
  | otherwise = unsafePerformIO $ do
      col <- G.tableGetColumn (_gTable table) (fromIntegral i)
      targetDType <- gGetDataType @a
      dType <- G.columnGetDataType col
      eq <- G.dataTypeEqual dType targetDType
      if eq
         then do 
           field <- G.columnGetField col
           name <- G.fieldGetName field
           ca <- boxChunkedArray <$> G.columnGetData col
           typeId <- G.dataTypeGetId dType
           return $ Right $ Column col ca (Field name field dType typeId)
         else do
           typeTxt <- G.dataTypeToString dType
           targetTypeTxt <- G.dataTypeToString targetDType
           return $ Left $ TypeMismatch $ "Couldn't match "<>targetTypeTxt
                                          <> "with actual type of "<>typeTxt

getField :: Int -> Table -> Either ArrowError Field
getField i table 
  | i < 0 = Left (IndexOutOfBounds i)
  | i >= _tableNumColumns table = Left (IndexOutOfBounds i)
  | otherwise = unsafePerformIO $ do
      col <- G.tableGetColumn (_gTable table) (fromIntegral i)
      dt <- G.columnGetDataType col
      dtId <- G.dataTypeGetId dt
      field <- G.columnGetField col
      name <- G.fieldGetName field
      return $ Right $ Field name field dt dtId


getChunk :: ArrowType a => Int -> ChunkedArray a -> Either ArrowError (Array a)
getChunk i chunks 
  | i < 0 = Left (IndexOutOfBounds i)
  | i >= _chunkedArrayNumChunks chunks = Left (IndexOutOfBounds i)
  | otherwise = Right $ unsafePerformIO $ do
      arr <- G.chunkedArrayGetChunk (_gChunkedArray chunks) (fromIntegral i)
      return $ unsafeCastArray arr


readFeatherFile :: Text -> IO Table
readFeatherFile path = do
  mm <- G.memoryMappedInputStreamNew path 
          >>= orThrow (AllocationError $ "Memory mapping file: "<>path)
  ffr <- G.featherFileReaderNew mm 
          >>= orThrow (AllocationError "FeatherFileReader")
  _gTable <- G.featherFileReaderRead ffr
  _tableNumColumns <- fromIntegral <$> G.tableGetNColumns _gTable
  return Table{..}


writeFeatherFile :: Table -> Text -> IO ()
writeFeatherFile table path = do
  fout <- G.fileOutputStreamNew path False
          >>= orThrow (AllocationError $ "Opening file for writing: "<>path)
  bracket (G.featherFileWriterNew fout
            >>= orThrow (AllocationError "FeatherFileWriter"))
          G.featherFileWriterClose
          (\ffw -> G.featherFileWriterWrite ffw $ _gTable table)

colFromArray :: forall a. ArrowType a => Text -> Array a -> Column a
colFromArray name arr = unsafePerformIO $ do
  dt <- coerce <$> gGetDataType @a
  typeId <- G.dataTypeGetId dt
  _gField <- G.fieldNew name dt
  let field = Field name _gField dt typeId
  chunk <- G.chunkedArrayNew [coerce arr :: GArray a]
  gField <- G.fieldNew name dt
  col <- G.columnNewChunkedArray gField chunk 
  return $ Column col (boxChunkedArray chunk) field


orThrow :: Exception e => e -> Maybe a -> IO a
orThrow e (Just a) = return a
orThrow e Nothing = throwIO e


colInfo :: Table -> [(Text, G.Type, Text)]
colInfo table = [(_fieldName, _fieldTypeId, unsafePerformIO $ G.dataTypeToString _fieldDataType) 
                | i <- [0.._tableNumColumns table - 1]
                , let Right Field{..} = getField i table]


newSchema :: [Field] -> Schema
newSchema fields = unsafePerformIO $ do
  _gSchema <- G.schemaNew $ _gField <$> fields
  return Schema {_gSchema, _schemaFields = fields}


newTable :: [SomeColumn] -> Table
newTable cols = unsafePerformIO $ do
  let schema = newSchema $ (untoSomeColumn _columnField <$> cols)
  _gTable <- (G.tableNewColumns (_gSchema schema) $ untoSomeColumn _gColumn <$> cols)
              >>= orThrow (AllocationError "Error allocating table")
  return Table {_gTable, _tableNumColumns = length cols}

