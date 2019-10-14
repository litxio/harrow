{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

module Data.Apache.Arrow.Conversion where

import qualified GI.Arrow as G
import System.IO.Unsafe
import qualified Debug.Trace as Debug
import Data.Int
import Data.Word
import Data.Text (Text)
import Control.Exception
import Data.GI.Base.Utils (allocMem, ptr_to_g_free, freeMem)
import Data.GI.Base (withManagedPtr, managedForeignPtr)
import GI.GLib.Structs.Bytes (bytesNew, bytesGetData)
import Control.Monad.IO.Class
import GHC.Stack
import Foreign.Storable
import Data.Foldable
import qualified Data.Vector.Storable as SV
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BS

import Data.Apache.Arrow.Types
import Data.Coerce
import Foreign.Ptr
import Foreign.ForeignPtr
import qualified Foreign.Concurrent as FC
import Foreign.Storable (peek)
import Data.Apache.Arrow.Table

-- | Pointer to `gobject_unref`.
foreign import ccall "&g_object_unref" ptr_to_g_object_unref :: FunPtr (Ptr a -> IO ())

-- Tricky: I have a ManagedPtr around a GArray; but I need a ForeignPtr to the
-- underlying array of values. 
toStorableVector :: forall a. (ArrowType a, Storable a)
                 => Array a -> SV.Vector a
toStorableVector arr = unsafePerformIO $ 
  withManagedPtr (coerce arr :: G.ManagedPtr (GArray a)) $ \ptr -> do
      length_ :: Ptr Int64 <- allocMem
      -- TODO this data doesn't seem to be getting released!!
      dataPtr :: Ptr a <- coerce <$> rawValuesPtr @a (coerce ptr) length_
      -- dataFPtr <- newForeignPtr ptr_to_g_object_unref dataPtr 
      dataFPtr <- newForeignPtr_ dataPtr 
      -- addForeignPtrFinalizer shouter dataFPtr
      -- FC.addForeignPtrFinalizer dataFPtr (G.unrefObject (coerce dataFPtr))
      len <- peek length_
      freeMem length_
      return $ SV.unsafeFromForeignPtr0 dataFPtr (fromIntegral len)

chunkedToStorableVector :: forall a. (ArrowType a, Storable a)
                        => ChunkedArray a -> SV.Vector a
chunkedToStorableVector ca = mconcat [toStorableVector $ fromRight $ getChunk i ca 
                                      | i <- [0.._chunkedArrayNumChunks ca-1]]
  where
    fromRight (Right a) = a
    fromRight (Left e) = error $ "Impossible! When getting chunk from array: "
                                 <>show e

printBytes :: BS.ByteString -> IO ()
printBytes bs = 
  for_ [0..BS.length bs-1] $ \i ->
    Debug.traceIO $ "["++show i ++"]: " ++ show (BS.index bs i)

fromStorableVector :: forall a. (Show a, ArrowType a, Storable a)
                   => SV.Vector a -> Array a
fromStorableVector vec = unsafePerformIO $ do
  -- Debug.traceIO "Entering fromStorableVector"
  let (ptr, len) = SV.unsafeToForeignPtr0 vec
      size = sizeOf (undefined :: a)
      dataBytes = BS.fromForeignPtr (coerce ptr) 0 (len * size)
  -- Debug.traceIO $ "Len is "++show len++", total bytes = "++show (len*size)
  -- withForeignPtr ptr $ \p -> peek p >>= Debug.traceIO . show
  -- FC.addForeignPtrFinalizer ptr (putStrLn "Finalizing ptr")
  -- Potential problem -- I now have a ForeignPtr to the data (in the vector)
  -- AND a separate ManagedPtr (in buf).  But it seems in practice, ptr is 
  -- a "PlainPtr" ForeignPtr - and thus has no finalizer anyway?
  -- Debug.traceIO $ "Length dataBytes is " ++ (show $ BS.length dataBytes)
  -- printBytes dataBytes

  -- TODO This doesn't seem to work - data is corrupted
  -- buf <- G.bufferNew dataBytes

  -- TODO -- avoid the copy here
  buf <- G.bufferNewBytes =<< bytesNew (Just dataBytes)
  -- G.bufferGetSize buf >>= \n -> Debug.traceIO $ "Buf size " ++ show n
  Just rtBS <- G.bufferGetData buf >>= bytesGetData

  -- Debug.traceIO $ "Length rtBS is " ++ (show $ BS.length rtBS)
  -- printBytes rtBS
  -- printBytes dataBytes
  -- FC.addForeignPtrFinalizer (managedForeignPtr $ coerce buf)
  --                           (Debug.traceIO "Finalizing buf")
  arr :: GArray a <- gArrayNew @a (fromIntegral $ SV.length vec)
                                  buf (Nothing :: Maybe G.Buffer) 0
  -- Debug.traceIO "Exiting from fromStorableVector"

  -- Go GArray a -> G.ManagedPtr (GArray a) -> Array a
  let res = coerce (coerce arr :: G.ManagedPtr (GArray a))
  return res
