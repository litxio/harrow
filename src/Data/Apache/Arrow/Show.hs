
{-# LANGUAGE StandaloneDeriving #-}

module Data.Apache.Arrow.Show where

import qualified GI.Arrow as G
import System.IO.Unsafe
import Data.Int
import Data.Word
import Data.Maybe (fromMaybe)
import Data.Text (Text, unpack)
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

instance Show G.Column where
  show = unpack 
          . fromMaybe "<error calling columnToString>"
          . unsafePerformIO 
          . G.columnToString

instance Show G.Table where
  show = unpack 
          . fromMaybe "<error calling tableToString>"
          . unsafePerformIO 
          . G.tableToString

instance Show G.ChunkedArray where
  show = unpack 
          . fromMaybe "<error calling chunkedArrayToString>"
          . unsafePerformIO 
          . G.chunkedArrayToString

instance Show G.Array where
  show = unpack 
          . fromMaybe "<error calling chunkedArrayToString>"
          . unsafePerformIO 
          . G.arrayToString

instance Show G.Field where
  show = unpack 
          . unsafePerformIO 
          . G.fieldToString

deriving instance Show Field

deriving instance Show (ChunkedArray a)

deriving instance Show a => Show (Column a)

deriving instance Show Table

instance Show G.DataType where
  show = unpack . unsafePerformIO . G.dataTypeToString
