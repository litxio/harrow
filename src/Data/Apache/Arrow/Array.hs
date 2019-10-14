{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}

module Data.Apache.Arrow.Array where

import qualified GI.Arrow as G
import System.IO.Unsafe
import Data.Int
import Data.Maybe (fromMaybe, isJust)
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
import Data.Apache.Arrow.Builder

fromListWithNulls :: forall a. (ArrowType a)
                  => [Maybe a] -> Array a
fromListWithNulls vals = buildArray $ appendValues vals

countNonNull :: forall a. ArrowType a => Array a -> Int64
countNonNull arr = unsafePerformIO $ do
  co <- G.countOptionsNew
  G.setCountOptionsMode co G.CountModeAll
  G.arrayCount (coerce arr :: G.Array) (Just co)

countNull :: forall a. ArrowType a => Array a -> Int64
countNull arr = unsafePerformIO $ do
  co <- G.countOptionsNew
  G.setCountOptionsMode co G.CountModeNull
  G.arrayCount (coerce arr :: G.Array) (Just co)

