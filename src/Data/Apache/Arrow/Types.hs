{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

module Data.Apache.Arrow.Types where

import qualified GI.Arrow as G
import System.IO.Unsafe
import Data.Int
import Data.Word
import Data.Text (Text)
import Control.Exception
import Data.GI.Base.Utils (allocMem)
import Data.GI.Base (managedForeignPtr)
import Control.Monad.IO.Class
import GHC.Stack
import qualified Data.Vector.Storable as SV
import Data.Coerce

import Foreign.Ptr
import Foreign.ForeignPtr
import Foreign.Storable (peek, Storable)

class (G.IsArray (GArray a), G.IsArrayBuilder (GArrayBuilder a)
      ,G.IsDataType (GDataType a)
      ,Coercible (GRep a) a
      ,Coercible (Array a) (G.ManagedPtr (GArray a))
      ,Coercible (G.ManagedPtr (GArray a)) (Array a)
      ,Coercible G.Array (Array a)
      ,Coercible (Array a) (GArray a))
  => ArrowType a where
  data Array a :: *
  -- | The Haskell type that Arrow uses as the equivalent of 'a' .. e.g. Int64
  -- for Date64
  type GRep a :: * 
  type GRep a = a
  -- The C array type
  type GArray a :: *
  -- The C array builder type
  type GArrayBuilder a :: * 
  -- For gAppendValues -- list for most types, but ByteString for Word8
  type GDataType a :: * 
  type Many a :: * 
  type Many a = [a]
  -- type NewBuilderArg a :: *
  -- type NewBuilderArg a = ()
  arrowTypeId :: G.Type
  gGetDataType :: IO (GDataType a)
  gArrayNew :: (G.IsBuffer b, G.IsBuffer c)
            => Int64 -> b -> Maybe c -> Int64 -> IO (GArray a)
  gArrayConstructor :: G.ManagedPtr (GArray a) -> GArray a
  gBuilderConstructor :: G.ManagedPtr (GArrayBuilder a) -> GArrayBuilder a
  -- unsafeCastArray :: G.Array -> Array a
  newGBuilder :: (HasCallStack) => IO (GArrayBuilder a)
  gAppendNull :: (HasCallStack) => GArrayBuilder a -> IO ()
  gAppendNulls :: (HasCallStack) => GArrayBuilder a -> Int64 -> IO ()
  gAppendValue :: (HasCallStack) => GArrayBuilder a -> GRep a -> IO ()
  gAppendValues :: (HasCallStack) 
               => GArrayBuilder a -> Many a -> Maybe [Bool] -> IO ()
  rawValuesPtr :: Ptr (GArray a) -> Ptr Int64 -> IO (Ptr (GRep a))
  -- This is used to fill buffer space at null locations
  placeholder :: a
  default placeholder :: Num a => a
  placeholder = 0
  listToMany :: [a] -> Many a
  default listToMany :: Coercible [a] (Many a) => [a] -> Many a
  listToMany = coerce


unsafeCastArray :: Coercible G.Array (Array a) => G.Array -> Array a
unsafeCastArray = coerce
unwrapArray :: ArrowType a => Array a -> GArray a
unwrapArray = coerce

data TimeUnit = Second | Milli | Micro | Nano

newtype Date32 = Date32 {daysSinceEpoch :: Int32}
  deriving (Eq, Ord, Show, Num, Enum, Real, Integral, Storable)
newtype Date64 = Date64 {millisSinceEpoch :: Int64 }
  deriving (Eq, Ord, Show, Num, Enum, Real, Integral, Storable)
newtype Time32 = Time32 {time32Time :: Int32}
  deriving (Eq, Ord, Show, Num, Enum, Real, Integral, Storable)
newtype Time64 = Time64 {time64Time :: Int64 }
  deriving (Eq, Ord, Show, Num, Enum, Real, Integral, Storable)
newtype Timestamp (u :: TimeUnit) = Timestamp {unitsSinceEpoch :: Int64 }
  deriving (Eq, Ord, Show, Num, Enum, Real, Integral, Storable)

-- instance ArrowType Int64 where
--   newtype Array Int64 = Int64Array {_gInt64Array :: G.Int64Array}
--   arrowTypeId = G.TypeInt64
--   unsafeCastArray = Int64Array . unsafePerformIO . G.unsafeCastTo G.Int64Array

data ArrowError = IndexOutOfBounds Int 
                | AllocationError Text
                | TypeMismatch Text
  deriving Show

instance Exception ArrowError

data Table = Table {_gTable :: G.Table
                   ,_tableNumColumns :: Int }

data Column a = Column { _gColumn :: G.Column
                       , _columnData :: ChunkedArray a
                       , _columnField :: Field }

data Schema = Schema {_gSchema :: G.Schema
                     ,_schemaFields :: [Field] }

data Field = Field { _fieldName :: Text
                   , _gField :: G.Field
                   , _fieldDataType :: G.DataType
                   , _fieldTypeId :: G.Type }

data ChunkedArray a = ChunkedArray { _gChunkedArray :: G.ChunkedArray 
                                   , _chunkedArrayLength :: Word64
                                   , _chunkedArrayNumChunks :: Int }

