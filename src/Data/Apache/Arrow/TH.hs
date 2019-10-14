{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE QuasiQuotes #-}

module Data.Apache.Arrow.TH where

import Language.Haskell.TH
import qualified GI.Arrow as G
import System.IO.Unsafe
import Data.Maybe (fromMaybe, isNothing, mapMaybe)
import Foreign.Ptr
import Data.Coerce
import Control.Monad.IO.Class
import Data.Char (toLower, toUpper)
import Data.Int

import Data.Apache.Arrow.Types

data ArrowConfigInstanceParams = 
  ACIP { elemT :: Name
       , elemArgT :: Maybe Name
       -- ^ if elemT is of kind k -> *, this is the arg of kind k.  Used for
       -- time units.
       , elemRepT :: Name
       , dataTypeT :: Name
       , newDataTypeArgV :: Maybe Name
       , manyElem :: Type
       , gArrayT :: Name
       , gBuilderT :: Name
       , newBuilderArgT :: Maybe Name
       -- ^ Defaults to () because most BuilderNew functions don't take an
       -- argument. But a few do, like those for dates and times (argument is
       -- the time unit)
       , arrowTypeV :: Name
       -- ^ A constructor of G.Type
       , declareForeignGetValues :: Bool
       -- ^ Should we import the garrow_xxx_array_get_values function? Set to
       -- false to avoid duplicate declarations
       , placeholderExp :: ExpQ
       , listToManyExp :: ExpQ
       }


defaultArrowInstanceParams :: Name -> ArrowConfigInstanceParams
defaultArrowInstanceParams elemT =
  let baseName = nameBase elemT
      in ACIP {elemT = elemT
              ,elemArgT = Nothing
              ,dataTypeT = mkName $ "G."++baseName++"DataType"
              ,newDataTypeArgV = Nothing
              ,elemRepT = elemT
              ,manyElem = AppT ListT (ConT elemT)
              ,gArrayT = mkName $ "G."++baseName++"Array"
              ,gBuilderT=mkName $ "G."++baseName++"ArrayBuilder"
              ,newBuilderArgT = Nothing
              ,arrowTypeV=mkName $ "G.Type"++baseName
              ,declareForeignGetValues = True
              ,placeholderExp = [| 0 |]
              ,listToManyExp = [| coerce |]
              }


mkArrowType :: ArrowConfigInstanceParams -> Q [Dec]
mkArrowType ACIP{..} = do
  let elemConT = case elemArgT of 
                   Nothing -> ConT elemT
                   Just arg -> AppT (ConT elemT) (PromotedT arg)
  let constructor= mkName $ nameBase elemT 
                            ++ (fromMaybe "" $ nameBase <$> elemArgT)
                            ++ "Array"
      deconstructor = mkName $ "_g"++nameBase elemT
                               ++ (fromMaybe "" $ nameBase <$> elemArgT)
                               ++ "Array"
  
  -- The array constructor should have the same name as the array type
  gCon <- (lookupValueName $ "G."++nameBase gArrayT) >>= \case
                Nothing -> fail $ "Bad constructor name: G."++nameBase gArrayT
                Just c -> return c

  -- Ditto for the builder
  gBCon <- (lookupValueName $ "G."++nameBase gBuilderT) >>= \case
                Nothing -> fail $ "Bad constructor name: G."++nameBase gBuilderT
                Just c -> return c

  newBuilder <- declareFunction gBuilderT "new" "newGBuilder" newBuilderArgT
  arrayNew <- declareFunction gArrayT "New" "gArrayNew" newBuilderArgT
  appendValue <- declareFunction gBuilderT "appendValue" "gAppendValue" Nothing
  appendValues <- declareFunction gBuilderT "appendValues" "gAppendValues" Nothing
  appendNull <- declareFunction gBuilderT "appendNull" "gAppendNull" Nothing
  appendNulls <- declareFunction gBuilderT "appendNulls" "gAppendNulls" Nothing
  
  -- let manyType = fromMaybe (AppT ListT (ConT repName)) mbManyType
  -- unsafeCast <- [|$(conE constructor) . unsafePerformIO . G.unsafeCastTo $(conE gCon)|]

  let dtNew = mkName $ "G."++lowerFirst (nameBase dataTypeT) ++ "New"
  getDataTypeImpl <- case newDataTypeArgV of 
                       Nothing -> varE dtNew
                       Just arg -> [| $(varE dtNew) $(conE arg) |]
  (foreignGetVals, getValsFDecl) <- mkForeignImportGetValues elemConT elemRepT gArrayT
  ph <- placeholderExp
  listToMany <- listToManyExp
  let decl =  
        [InstanceD Nothing [] ( AppT (ConT ''ArrowType) elemConT ) 
          [ NewtypeInstD [] (mkName "Array") [ elemConT ] Nothing 
              ( RecC constructor
                  [ 
                      ( deconstructor
                      , Bang NoSourceUnpackedness NoSourceStrictness
                      , ConT gArrayT
                      ) 
                  ]
              ) []
          , TySynInstD ''GArrayBuilder (TySynEqn [ elemConT ]
                                                 ( ConT gBuilderT ))
          , TySynInstD ''GArray (TySynEqn [ elemConT ]
                                          ( ConT gArrayT ))
          , TySynInstD ''GRep (TySynEqn [ elemConT ]
                                        ( ConT elemRepT ))
          , TySynInstD ''GDataType (TySynEqn [ elemConT ] (ConT dataTypeT))
          , TySynInstD ''Many (TySynEqn [ elemConT ] manyElem)
          -- , TySynInstD ''NewBuilderArg (TySynEqn [ elemConT ]
          --                                        (fromMaybe (TupleT 0) newBuilderArg))
          , ValD ( VarP $ mkName "arrowTypeId" ) 
              ( NormalB ( ConE arrowTypeV ) ) []
          , ValD ( VarP $ mkName "gArrayConstructor" ) 
              ( NormalB ( ConE gCon ) ) []
          , ValD ( VarP $ mkName "gBuilderConstructor" ) 
                 ( NormalB ( ConE gBCon ) ) []
          -- , ValD ( VarP $ mkName "unsafeCastArray" ) 
          --        ( NormalB unsafeCast ) []
          , ValD ( VarP $ mkName "gGetDataType" ) 
                 ( NormalB getDataTypeImpl ) []
          , arrayNew
          , newBuilder
          , appendValue
          , appendValues
          , appendNull
          , appendNulls
          , ValD (VarP $ mkName "rawValuesPtr")
                 (NormalB (VarE foreignGetVals)) []
          , ValD (VarP $ mkName "placeholder")
                 (NormalB ph) []
          , ValD (VarP $ mkName "listToMany")
                 (NormalB listToMany) []
          ]]
  if declareForeignGetValues 
     then return $ decl ++ [getValsFDecl]
     else return decl

forceIO :: IO a -> IO a
forceIO = id

declareFunction :: Name -> String -> String -> Maybe Name -> Q Dec
declareFunction base origFunSuffix instFunName mbArg = do
  let gFunNameStr = "G."++lowerFirst (nameBase base)++upperFirst origFunSuffix
  gFunName <- lookupValueName gFunNameStr >>= \case
                      Nothing -> fail $ "Bad function name: "++gFunNameStr
                      Just c -> return c
  impl <- case mbArg of
            Just arg -> [| $(varE gFunName) $(varE arg) |] 
            Nothing  -> [| $(varE gFunName) |]
  return $ ValD ( VarP $ mkName instFunName ) ( NormalB impl ) []

lowerFirst [] = []
lowerFirst (c:cs) = toLower c : cs

upperFirst [] = []
upperFirst (c:cs) = toUpper c : cs

mkForeignImportGetValues :: Type -> Name -> Name -> Q (Name, Dec)
mkForeignImportGetValues elemConT elemRepT gArrTypeName = do
  let gArrTypeStr = nameBase gArrTypeName 
      -- "Strip off the "Array" suffix
      gBaseName = take (length gArrTypeStr - 5) $ nameBase gArrTypeName
      foreignNameStr = "garrow_"++(toLower <$> gBaseName)++"_array_get_values"
      foreignName = mkName foreignNameStr
  typeSig <- [t| Ptr $(conT gArrTypeName)
                 -> Ptr Int64 
                 -> IO (Ptr $(conT elemRepT)) |]

  return (foreignName
         ,ForeignD ( ImportF CCall Safe ("static "++foreignNameStr)
                                        foreignName typeSig ))



mkSumWrapper :: Name -> Name -> String -> String -> Q [Dec]
mkSumWrapper tcon cls wrapperName ctrPrefix = do
  instanceDecs <- reifyInstances cls [VarT $ mkName "a"]
  let toSomeClassName = mkName $ "To"++wrapperName
      toSomeFnName = mkName $ "to"++wrapperName
      f = mkName "f"
      x = mkName "x"
      mkConstructor (instType, ctrName) =
        NormalC ctrName [ ( Bang NoSourceUnpackedness NoSourceStrictness
                          , AppT ( ConT tcon ) instType) ]
      mkUntoClause (_, ctrName) =
        Clause [VarP f, ConP ctrName [VarP x]]
               (NormalB (AppE (VarE f) (VarE x))) []
      mkToSomeInstance (instType, ctrName) =
        InstanceD Nothing [] (AppT (ConT toSomeClassName) instType)
                  [ValD (VarP toSomeFnName) (NormalB (ConE ctrName)) []]
      extractNames :: InstanceDec -> Maybe (Type, Name)
      extractNames = \case
            InstanceD _ _ (AppT _ (ConT tn)) _ -> 
              let ctrName = mkName $ ctrPrefix ++ nameBase tn
               in Just (ConT tn, ctrName)
            InstanceD _ _ (AppT c (AppT (ConT tn) (PromotedT k))) _ -> 
              let ctrName = mkName $ ctrPrefix ++ nameBase tn ++ nameBase k
               in Just (AppT (ConT tn) (PromotedT k), ctrName)
            --    in Just $ (NormalC ctrName
            --               [ 
            --                   ( Bang NoSourceUnpackedness NoSourceStrictness
            --                   , AppT ( ConT tcon ) (AppT (ConT tn) (PromotedT k))
            --                   ) 
            --               ]
            --              ,Clause [VarP f, ConP ctrName [VarP x]]
            --                      (NormalB (AppE (VarE f) (VarE x))) [])
            --                
            _ -> Nothing
      matches = mapMaybe extractNames instanceDecs
  -- for instanceDecs -> (ctrs, untos) = unzip $ mapMaybe mkCtrAndUnto instanceDecs

  let untoName = mkName $ "unto"++wrapperName
      a = mkName "a"
      b = mkName "b"
      wrapper = mkName wrapperName
  -- untoSig <- [t| (forall . $(conT cls) $(a) => $(conT tcon) $(a) -> $(b))
  --                -> $(conT $ mkName wrapperName)
  --                -> $(b) |]
  let untoSig = SigD untoName 
        (AppT 
            ( AppT ArrowT 
                ( ForallT [ PlainTV a ] 
                    [ AppT ( ConT cls) ( VarT a ) ] 
                    ( AppT 
                        ( AppT ArrowT 
                            ( AppT ( ConT tcon ) ( VarT a ) )
                        ) ( VarT b )))) 
            ( AppT 
                ( AppT ArrowT ( ConT wrapper )) ( VarT b )))
                
  return $ [ DataD [] wrapper [] Nothing (mkConstructor <$> matches) []
           , untoSig
           , FunD untoName $ mkUntoClause <$> matches
           , ClassD [] toSomeClassName [ PlainTV a ] [] 
              [ SigD toSomeFnName (AppT ( AppT ArrowT ( AppT (ConT tcon) (VarT a)))
                                        ( ConT wrapper )) ]
           ] ++ (mkToSomeInstance <$> matches)
