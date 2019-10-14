{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}

module Data.Apache.Arrow.Instances where

import GHC.Int
import Data.ByteString (ByteString, pack)

import System.IO.Unsafe
import Data.Maybe
import Language.Haskell.TH (Type(..))
import Data.Apache.Arrow.TH
import Data.Apache.Arrow.Types
import qualified GI.Arrow as G
import GHC.Word

mkArrowType $ (defaultArrowInstanceParams ''Bool) {gArrayT = ''G.BooleanArray
                                                  ,dataTypeT = ''G.BooleanDataType
                                                  ,gBuilderT = ''G.BooleanArrayBuilder
                                                  ,arrowTypeV = 'G.TypeBoolean
                                                  ,placeholderExp = [|False|] }


mkArrowType $ (defaultArrowInstanceParams ''Int8)
mkArrowType $ (defaultArrowInstanceParams ''Int16)
mkArrowType $ (defaultArrowInstanceParams ''Int32)
mkArrowType $ (defaultArrowInstanceParams ''Int64)

mkArrowType $ (defaultArrowInstanceParams ''Word8) {manyElem = ConT ''ByteString
                                                   ,dataTypeT = ''G.UInt8DataType
                                                   ,arrowTypeV = 'G.TypeUint8
                                                   ,gArrayT = ''G.UInt8Array
                                                   ,gBuilderT = ''G.UInt8ArrayBuilder
                                                   ,listToManyExp = [| pack |]}
mkArrowType $ (defaultArrowInstanceParams ''Word16) {gArrayT = ''G.UInt16Array
                                                    ,dataTypeT = ''G.UInt16DataType
                                                    ,arrowTypeV = 'G.TypeUint16
                                                    ,gBuilderT = ''G.UInt16ArrayBuilder}
mkArrowType $ (defaultArrowInstanceParams ''Word32) {gArrayT = ''G.UInt32Array
                                                    ,dataTypeT = ''G.UInt32DataType
                                                    ,arrowTypeV = 'G.TypeUint64
                                                    ,gBuilderT = ''G.UInt32ArrayBuilder}
mkArrowType $ (defaultArrowInstanceParams ''Word64) {gArrayT = ''G.UInt64Array
                                                    ,dataTypeT = ''G.UInt64DataType
                                                    ,arrowTypeV = 'G.TypeUint64
                                                    ,gBuilderT = ''G.UInt64ArrayBuilder}

mkArrowType $ (defaultArrowInstanceParams ''Double)
mkArrowType $ (defaultArrowInstanceParams ''Float)
-- 
-- -- TODO half float?
-- -- TODO binary
-- -- TODO fixed binary
-- -- TODO Decimal128
-- 
mkArrowType $ (defaultArrowInstanceParams ''Date32) {elemRepT = ''Int32
                                                    ,manyElem = AppT ListT (ConT ''Int32) }
mkArrowType $ (defaultArrowInstanceParams ''Date64) {elemRepT = ''Int64
                                                    ,manyElem = AppT ListT (ConT ''Int64) }

timestampDataTypeSecond :: G.TimestampDataType
timestampDataTypeSecond = unsafePerformIO $ G.timestampDataTypeNew G.TimeUnitSecond

timestampDataTypeMilli :: G.TimestampDataType
timestampDataTypeMilli = unsafePerformIO $ G.timestampDataTypeNew G.TimeUnitMilli

timestampDataTypeMicro :: G.TimestampDataType
timestampDataTypeMicro = unsafePerformIO $ G.timestampDataTypeNew G.TimeUnitMicro

timestampDataTypeNano :: G.TimestampDataType
timestampDataTypeNano = unsafePerformIO $ G.timestampDataTypeNew G.TimeUnitNano

mkArrowType $ (defaultArrowInstanceParams ''Timestamp)
                {elemRepT = ''Int64
                ,elemArgT = Just 'Second
                ,newDataTypeArgV = Just 'G.TimeUnitSecond
                ,manyElem = AppT ListT (ConT ''Int64) 
                ,newBuilderArgT = Just 'timestampDataTypeSecond}

mkArrowType $ (defaultArrowInstanceParams ''Timestamp)
                {elemRepT = ''Int64
                ,elemArgT = Just 'Milli
                ,newDataTypeArgV = Just 'G.TimeUnitMilli
                ,manyElem = AppT ListT (ConT ''Int64) 
                ,newBuilderArgT = Just 'timestampDataTypeMilli
                ,declareForeignGetValues = False}

mkArrowType $ (defaultArrowInstanceParams ''Timestamp)
                {elemRepT = ''Int64
                ,elemArgT = Just 'Micro
                ,newDataTypeArgV = Just 'G.TimeUnitMicro
                ,manyElem = AppT ListT (ConT ''Int64) 
                ,newBuilderArgT = Just 'timestampDataTypeMicro
                ,declareForeignGetValues = False}

mkArrowType $ (defaultArrowInstanceParams ''Timestamp)
                {elemRepT = ''Int64
                ,elemArgT = Just 'Nano
                ,newDataTypeArgV = Just 'G.TimeUnitNano
                ,manyElem = AppT ListT (ConT ''Int64) 
                ,newBuilderArgT = Just 'timestampDataTypeNano
                ,declareForeignGetValues = False}
-- -- TODO Tensor


mkSumWrapper ''Column ''ArrowType "SomeColumn" "Col"
