{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

module Data.Apache.Arrow.Builder where

import qualified GI.Arrow as G
import System.IO.Unsafe
import Data.Maybe
import Data.Int
import Data.Word
import Data.Text (Text)
import Control.Exception
import Data.GI.Base.Utils (allocMem)
import Data.GI.Base (managedForeignPtr)
import qualified Data.Vector.Storable as SV
import Control.Monad (ap)
import Data.Coerce

import Foreign.Ptr
import Foreign.ForeignPtr
import Foreign.Storable (peek)

import Control.Monad.Trans.Reader
import Data.Apache.Arrow.Types

newtype ArrayBuilder b a = ArrayBuilder { runArrayBuilder :: GArrayBuilder b -> IO a }

instance Functor (ArrayBuilder b) where
  fmap f (ArrayBuilder g) = ArrayBuilder $ \ab -> f <$> g ab

instance Applicative (ArrayBuilder b) where
  pure = return
  (<*>) = ap

instance Monad (ArrayBuilder b) where
  return a = ArrayBuilder $ const (return a)
  (>>=) (ArrayBuilder g) f = ArrayBuilder $ \ab -> g ab >>= \a -> runArrayBuilder (f a) ab

appendValue :: forall b. ArrowType b => b -> ArrayBuilder b ()
appendValue b = ArrayBuilder $ \ab -> gAppendValue @b ab (coerce b)

appendValues :: forall b. (ArrowType b)
             => [Maybe b] -> ArrayBuilder b ()
appendValues vals = ArrayBuilder $ \ab -> do
  let vals' = listToMany $ fromMaybe placeholder <$> vals
  gAppendValues @b ab vals' (Just $ isJust <$> vals)

buildArray :: forall a b. (ArrowType b)
           => ArrayBuilder b a -> Array b
buildArray (ArrayBuilder go) = unsafePerformIO $ do
  gBuilder <- newGBuilder @b
  b <- go gBuilder                             ::  IO a
  arr <- G.arrayBuilderFinish gBuilder         :: IO (G.Array)
  -- arrTyped <- G.unsafeCastTo (gArrayConstructor @a) arr :: IO (GArray b)
  return $ unsafeCastArray arr
