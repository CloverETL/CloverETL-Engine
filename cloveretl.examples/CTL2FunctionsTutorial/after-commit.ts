<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="CTL2FunctionsTutorial" description="CTL2 Functions Tutorial graphs" useJMX="true">    

<FunctionalTest ident="containerAppend" graphFile="graph/containerAppend.grf">
	 <FlatFile outputFile="data-out/containerAppendOverview.txt" supposedFile="supposed-out/containerAppendOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="containerClearList" graphFile="graph/containerClearList.grf">
	 <FlatFile outputFile="data-out/containerClearListOverview.txt" supposedFile="supposed-out/containerClearListOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="containerClearMap" graphFile="graph/containerClearMap.grf">
	 <FlatFile outputFile="data-out/containerClearMapOverview.txt" supposedFile="supposed-out/containerClearMapOverview.txt"/>
</FunctionalTest>
 
<FunctionalTest ident="containerCopy" graphFile="graph/containerCopy.grf">
	 <FlatFile outputFile="data-out/containerCopyOverview.txt" supposedFile="supposed-out/containerCopyOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="containerInsertElements" graphFile="graph/containerInsertElements.grf">
	 <FlatFile outputFile="data-out/containerInsertElementsOverview.txt" supposedFile="supposed-out/containerInsertElementsOverview.txt"/>	                                                                    
</FunctionalTest>

<FunctionalTest ident="containerInsertList" graphFile="graph/containerInsertList.grf">
	 <FlatFile outputFile="data-out/containerInsertListOverview.txt" supposedFile="supposed-out/containerInsertListOverview.txt"/>	                                                                    
</FunctionalTest>

<FunctionalTest ident="containerPoll" graphFile="graph/containerPoll.grf">
	 <FlatFile outputFile="data-out/containerPollOverview.txt" supposedFile="supposed-out/containerPollOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="containerPop" graphFile="graph/containerPop.grf">
	 <FlatFile outputFile="data-out/containerPopOverview.txt" supposedFile="supposed-out/containerPopOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="containerPush" graphFile="graph/containerPush.grf">
	 <FlatFile outputFile="data-out/containerPushOverview.txt" supposedFile="supposed-out/containerPushOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="containerRemove" graphFile="graph/containerRemove.grf">
	 <FlatFile outputFile="data-out/containerRemoveOverview.txt" supposedFile="supposed-out/containerRemoveOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="containerReverse" graphFile="graph/containerReverse.grf">
	<FlatFile outputFile="data-out/containerReverseOverview.txt" supposedFile="supposed-out/containerReverseOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="containerSort" graphFile="graph/containerSort.grf">
	 <FlatFile outputFile="data-out/containerSortOverview.txt" supposedFile="supposed-out/containerSortOverview.txt"/>
</FunctionalTest>


<FunctionalTest ident="conversionBase64byte" graphFile="graph/conversionBase64byte.grf">
	 <FlatFile outputFile="data-out/conversionBase64byteOverview.txt" supposedFile="supposed-out/conversionBase64byteOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionBits2str" graphFile="graph/conversionBits2str.grf">
	 <FlatFile outputFile="data-out/conversionBits2strOverview.txt" supposedFile="supposed-out/conversionBits2strOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionBool2num" graphFile="graph/conversionBool2num.grf">
	 <FlatFile outputFile="data-out/conversionBool2numOverview.txt" supposedFile="supposed-out/conversionBool2numOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionByte2base64" graphFile="graph/conversionByte2base64.grf">
	 <FlatFile outputFile="data-out/conversionByte2base64Overview.txt" supposedFile="supposed-out/conversionByte2base64Overview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionByte2hex" graphFile="graph/conversionByte2hex.grf">
	 <FlatFile outputFile="data-out/conversionByte2hexOverview.txt" supposedFile="supposed-out/conversionByte2hexOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionDate2long" graphFile="graph/conversionDate2long.grf">
	 <FlatFile outputFile="data-out/conversionDate2longOverview.txt" supposedFile="supposed-out/conversionDate2longOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionDate2num" graphFile="graph/conversionDate2num.grf">
	 <FlatFile outputFile="data-out/conversionDate2numOverview.txt" supposedFile="supposed-out/conversionDate2numOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionDate2str" graphFile="graph/conversionDate2str.grf">	 
	 <FlatFile outputFile="data-out/conversionDate2strOverview.txt" supposedFile="supposed-out/conversionDate2strOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionDecimal2double" graphFile="graph/conversionDecimal2double.grf">	 
	 <FlatFile outputFile="data-out/conversionDecimal2doubleOverview.txt" supposedFile="supposed-out/conversionDecimal2doubleOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionDecimal2integer" graphFile="graph/conversionDecimal2integer.grf" assertion="false">	 
	 <FlatFile outputFile="data-out/conversionDecimal2integerAllowedOverview.txt" supposedFile="supposed-out/conversionDecimal2integerAllowedOverview.txt"/>
	 <FlatFile outputFile="data-out/conversionDecimal2integerNotAllowedOverview.txt" supposedFile="supposed-out/conversionDecimal2integerNotAllowedOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionDecimal2long" graphFile="graph/conversionDecimal2long.grf" assertion="false">	 
	 <FlatFile outputFile="data-out/conversionDecimal2longAllowedOverview.txt" supposedFile="supposed-out/conversionDecimal2longAllowedOverview.txt"/>
	 <FlatFile outputFile="data-out/conversionDecimal2longNotAllowedOverview.txt" supposedFile="supposed-out/conversionDecimal2longNotAllowedOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionDouble2integer" graphFile="graph/conversionDouble2integer.grf" assertion="false">	 
	 <FlatFile outputFile="data-out/conversionDouble2integerAllowedOverview.txt" supposedFile="supposed-out/conversionDouble2integerAllowedOverview.txt"/>
	 <FlatFile outputFile="data-out/conversionDouble2integerNotAllowedOverview.txt" supposedFile="supposed-out/conversionDouble2integerNotAllowedOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionDouble2long" graphFile="graph/conversionDouble2long.grf" assertion="false">	 
	 <FlatFile outputFile="data-out/conversionDouble2longAllowedOverview.txt" supposedFile="supposed-out/conversionDouble2longAllowedOverview.txt"/>
	 <FlatFile outputFile="data-out/conversionDouble2longNotAllowedOverview.txt" supposedFile="supposed-out/conversionDouble2longNotAllowedOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionGetFieldName" graphFile="graph/conversionGetFieldName.grf">
	 <FlatFile outputFile="data-out/conversionGetFieldNameOverview.txt" supposedFile="supposed-out/conversionGetFieldNameOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionGetFieldType" graphFile="graph/conversionGetFieldType.grf">
	 <FlatFile outputFile="data-out/conversionGetFieldTypeOverview.txt" supposedFile="supposed-out/conversionGetFieldTypeOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionHex2byte" graphFile="graph/conversionHex2byte.grf">
	 <FlatFile outputFile="data-out/conversionHex2byteOverview.txt" supposedFile="supposed-out/conversionHex2byteOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionLong2date" graphFile="graph/conversionLong2date.grf">
	 <FlatFile outputFile="data-out/conversionLong2dateOverview.txt" supposedFile="supposed-out/conversionLong2dateOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionLong2integer" graphFile="graph/conversionLong2integer.grf" assertion="false">
	 <FlatFile outputFile="data-out/conversionLong2integerAllowedOverview.txt" supposedFile="supposed-out/conversionLong2integerAllowedOverview.txt"/>
	 <FlatFile outputFile="data-out/conversionLong2integerNotAllowedOverview.txt" supposedFile="supposed-out/conversionLong2integerNotAllowedOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionLong2packDecimal" graphFile="graph/conversionLong2packDecimal.grf">
	 <FlatFile outputFile="data-out/conversionLong2packDecimalOverview.txt" supposedFile="supposed-out/conversionLong2packDecimalOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionMd5FromBytes" graphFile="graph/conversionMd5FromBytes.grf">
	 <FlatFile outputFile="data-out/conversionMd5FromBytesOverview.txt" supposedFile="supposed-out/conversionMd5FromBytesOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionMd5FromString" graphFile="graph/conversionMd5FromString.grf">
	 <FlatFile outputFile="data-out/conversionMd5FromStringOverview.txt" supposedFile="supposed-out/conversionMd5FromStringOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionNum2bool" graphFile="graph/conversionNum2bool.grf">
	 <FlatFile outputFile="data-out/conversionNum2boolOverview.txt" supposedFile="supposed-out/conversionNum2boolOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionNum2str_FormatLocale" graphFile="graph/conversionNum2str_FormatLocale.grf">
	 <FlatFile outputFile="data-out/conversionNum2stringFormatOverview.txt" supposedFile="supposed-out/conversionNum2stringFormatOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionNum2strINTEGER" graphFile="graph/conversionNum2strINTEGER.grf">
	 <FlatFile outputFile="data-out/conversionNum2stringINTEGEROverview.txt" supposedFile="supposed-out/conversionNum2stringINTEGEROverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionNum2strLONG" graphFile="graph/conversionNum2strLONG.grf">
	 <FlatFile outputFile="data-out/conversionNum2stringLONGOverview.txt" supposedFile="supposed-out/conversionNum2stringLONGOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionNum2strNUMBER" graphFile="graph/conversionNum2strNUMBER.grf">
	 <FlatFile outputFile="data-out/conversionNum2stringNUMBEROverview.txt" supposedFile="supposed-out/conversionNum2stringNUMBEROverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionPackDecimal2long" graphFile="graph/conversionPackDecimal2long.grf">
	 <FlatFile outputFile="data-out/conversionPackDecimal2longOverview.txt" supposedFile="supposed-out/conversionPackDecimal2longOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionShaFromBytes" graphFile="graph/conversionShaFromBytes.grf">
	 <FlatFile outputFile="data-out/conversionShaFromBytesOverview.txt" supposedFile="supposed-out/conversionShaFromBytesOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionShaFromString" graphFile="graph/conversionShaFromString.grf">
	 <FlatFile outputFile="data-out/conversionShaFromStringOverview.txt" supposedFile="supposed-out/conversionShaFromStringOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionStr2bits" graphFile="graph/conversionStr2bits.grf">
	 <FlatFile outputFile="data-out/conversionStr2bitsOverview.txt" supposedFile="supposed-out/conversionStr2bitsOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionStr2bool" graphFile="graph/conversionStr2bool.grf">
	 <FlatFile outputFile="data-out/conversionStr2boolOverview.txt" supposedFile="supposed-out/conversionStr2boolOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionStr2datePatternLocale" graphFile="graph/conversionStr2datePatternLocale.grf">
	 <FlatFile outputFile="data-out/conversionStr2datePatternLocale0Overview.txt" supposedFile="supposed-out/conversionStr2datePatternLocale0Overview.txt"/>
	 <FlatFile outputFile="data-out/conversionStr2datePatternLocale1Overview.txt" supposedFile="supposed-out/conversionStr2datePatternLocale1Overview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionStr2integerWithFormatLocale" graphFile="graph/conversionStr2integerWithFormatLocale.grf">
	 <FlatFile outputFile="data-out/conversionStr2integerWithFormatLocale_OnlyDigitsOverview.txt" supposedFile="supposed-out/conversionStr2integerWithFormatLocale_OnlyDigitsOverview.txt"/>
	 <FlatFile outputFile="data-out/conversionStr2integerWithFormatLocale_ScientificNotationOverview.txt" supposedFile="supposed-out/conversionStr2integerWithFormatLocale_ScientificNotationOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionStr2integerWithRadix" graphFile="graph/conversionStr2integerWithRadix.grf">
	 <FlatFile outputFile="data-out/conversionStr2integerWithRadixOverview.txt" supposedFile="supposed-out/conversionStr2integerWithRadixOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionStr2longWithRadix" graphFile="graph/conversionStr2longWithRadix.grf">
	 <FlatFile outputFile="data-out/conversionStr2longWithRadixOverview.txt" supposedFile="supposed-out/conversionStr2longWithRadixOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionToString" graphFile="graph/conversionToString.grf">
	 <FlatFile outputFile="data-out/conversionToStringOverview.txt" supposedFile="supposed-out/conversionToStringOverview.txt"/>
</FunctionalTest>


<FunctionalTest ident="dateDateAdd" graphFile="graph/dateDateAdd.grf">
	 <FlatFile outputFile="data-out/dateDateAddOverview.txt" supposedFile="supposed-out/dateDateAddOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateDateDiff" graphFile="graph/dateDateDiff.grf">
	 <FlatFile outputFile="data-out/dateDateDiffOverview.txt" supposedFile="supposed-out/dateDateDiffOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateExtractDate" graphFile="graph/dateExtractDate.grf">
	 <FlatFile outputFile="data-out/dataExtractDateOverview.txt" supposedFile="supposed-out/dataExtractDateOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateExtractTime" graphFile="graph/dateExtractTime.grf">
	 <FlatFile outputFile="data-out/dataExtractTimeOverview.txt" supposedFile="supposed-out/dataExtractTimeOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateRandomDate_DateArguments" graphFile="graph/dateRandomDate_DateArguments.grf">
	 <FlatFile outputFile="data-out/dateRandomDate_DateArgumentsOverview.txt" supposedFile="supposed-out/dateRandomDate_DateArgumentsOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateRandomDate_LongArguments" graphFile="graph/dateRandomDate_LongArguments.grf">
	 <FlatFile outputFile="data-out/dateRandomDate_LongArgumentsOverview.txt" supposedFile="supposed-out/dateRandomDate_LongArgumentsOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateRandomDate_StringArguments" graphFile="graph/dateRandomDate_StringArguments.grf">
	 <FlatFile outputFile="data-out/dateRandomDate_StringArgumentsOverview.txt" supposedFile="supposed-out/dateRandomDate_StringArgumentsOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateToday" graphFile="graph/dateToday.grf">
</FunctionalTest>

<FunctionalTest ident="dateTruncDate" graphFile="graph/dateTruncDate.grf">
	 <FlatFile outputFile="data-out/dateTruncDateOverview.txt" supposedFile="supposed-out/dateTruncDateOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateTrunc" graphFile="graph/dateTrunc.grf">
	 <FlatFile outputFile="data-out/dateTruncOverview.txt" supposedFile="supposed-out/dateTruncOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateZeroDate" graphFile="graph/dateZeroDate.grf">
	 <FlatFile outputFile="data-out/dateZeroDateOverview.txt" supposedFile="supposed-out/dateZeroDateOverview.txt"/>
</FunctionalTest>


<FunctionalTest ident="miscellaneousIif" graphFile="graph/miscellaneousIif.grf">
	 <FlatFile outputFile="data-out/miscellaneousIifOverview.txt" supposedFile="supposed-out/miscellaneousIifOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="miscellaneousIsnull" graphFile="graph/miscellaneousIsnull.grf">
	 <FlatFile outputFile="data-out/miscellaneousIsnullOverview.txt" supposedFile="supposed-out/miscellaneousIsnullOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="miscellaneousNvl" graphFile="graph/miscellaneousNvl.grf">
	 <FlatFile outputFile="data-out/miscellaneousNvlOverview.txt" supposedFile="supposed-out/miscellaneousNvlOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="miscellaneousNvl2" graphFile="graph/miscellaneousNvl2.grf">
	 <FlatFile outputFile="data-out/miscellaneousNvl2Overview.txt" supposedFile="supposed-out/miscellaneousNvl2Overview.txt"/>
</FunctionalTest>

<FunctionalTest ident="miscellaneousPrintErr" graphFile="graph/miscellaneousPrintErr.grf">
	 <FlatFile outputFile="data-out/miscellaneousPrintErrOverview.txt" supposedFile="supposed-out/miscellaneousPrintErrOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="miscellaneousPrintLog" graphFile="graph/miscellaneousPrintLog.grf">
	 <FlatFile outputFile="data-out/miscellaneousPrintLog.txt" supposedFile="supposed-out/miscellaneousPrintLog.txt"/>
</FunctionalTest>


<FunctionalTest ident="stringCharAt" graphFile="graph/stringCharAt.grf">
	 <FlatFile outputFile="data-out/stringCharAtOverview.txt" supposedFile="supposed-out/stringCharAtOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringChop1Argument" graphFile="graph/stringChop1Argument.grf">
	 <FlatFile outputFile="data-out/stringChop1ArgumentOverview.txt" supposedFile="supposed-out/stringChop1ArgumentOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringChop2Arguments" graphFile="graph/stringChop2Arguments.grf">
	 <FlatFile outputFile="data-out/stringChop2ArgumentsOverview.txt" supposedFile="supposed-out/stringChop2ArgumentsOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringConcat" graphFile="graph/stringConcat.grf">
	 <FlatFile outputFile="data-out/stringConcatOverview.txt" supposedFile="supposed-out/stringConcatOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringCountChar" graphFile="graph/stringCountChar.grf">
	 <FlatFile outputFile="data-out/stringCountCharOverview.txt" supposedFile="supposed-out/stringCountCharOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringCut" graphFile="graph/stringCut.grf">
	 <FlatFile outputFile="data-out/stringCutOverview.txt" supposedFile="supposed-out/stringCutOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringEditDistanceIDENTICAL" graphFile="graph/stringEditDistanceIDENTICAL.grf">
	 <FlatFile outputFile="data-out/stringEditDistanceIDENTICALOverview.txt" supposedFile="supposed-out/stringEditDistanceIDENTICALOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringEditDistancePRIMARY" graphFile="graph/stringEditDistancePRIMARY.grf">
	 <FlatFile outputFile="data-out/stringEditDistancePRIMARYOverview.txt" supposedFile="supposed-out/stringEditDistancePRIMARYOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringEditDistanceSECONDARY" graphFile="graph/stringEditDistanceSECONDARY.grf">
	 <FlatFile outputFile="data-out/stringEditDistanceSECONDARYOverview.txt" supposedFile="supposed-out/stringEditDistanceSECONDARYOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringEditDistanceTERTIARY" graphFile="graph/stringEditDistanceTERTIARY.grf">
	 <FlatFile outputFile="data-out/stringEditDistanceTERTIARYOverview.txt" supposedFile="supposed-out/stringEditDistanceTERTIARYOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringFind" graphFile="graph/stringFind.grf">
	 <FlatFile outputFile="data-out/stringFindOverview.txt" supposedFile="supposed-out/stringFindOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringGetAlphanumericChars" graphFile="graph/stringGetAlphanumericChars.grf">
	 <FlatFile outputFile="data-out/stringGetAlphanumericCharsOverview.txt" supposedFile="supposed-out/stringGetAlphanumericCharsOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIndexOf" graphFile="graph/stringIndexOf.grf">
	 <FlatFile outputFile="data-out/stringIndexOfOverview.txt" supposedFile="supposed-out/stringIndexOfOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIsAscii" graphFile="graph/stringIsAscii.grf">
	 <FlatFile outputFile="data-out/stringIsAsciiOverview.txt" supposedFile="supposed-out/stringIsAsciiOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIsBlank" graphFile="graph/stringIsBlank.grf">
	 <FlatFile outputFile="data-out/stringIsBlankOverview.txt" supposedFile="supposed-out/stringIsBlankOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIsDate" graphFile="graph/stringIsDate.grf">
	 <FlatFile outputFile="data-out/stringIsDate0Overview.txt" supposedFile="supposed-out/stringIsDate0Overview.txt"/>
	 <FlatFile outputFile="data-out/stringIsDate1Overview.txt" supposedFile="supposed-out/stringIsDate1Overview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIsInteger" graphFile="graph/stringIsInteger.grf">
	 <FlatFile outputFile="data-out/stringIsIntegerOverview.txt" supposedFile="supposed-out/stringIsIntegerOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIsLong" graphFile="graph/stringIsLong.grf">
	 <FlatFile outputFile="data-out/stringIsLongOverview.txt" supposedFile="supposed-out/stringIsLongOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIsNumber" graphFile="graph/stringIsNumber.grf">
	 <FlatFile outputFile="data-out/stringIsNumberOverview.txt" supposedFile="supposed-out/stringIsNumberOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringJoinList" graphFile="graph/stringJoinList.grf">
	 <FlatFile outputFile="data-out/stringJoinListOverview.txt" supposedFile="supposed-out/stringJoinListOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringJoinMap" graphFile="graph/stringJoinMap.grf">
	 <FlatFile outputFile="data-out/stringJoinMapOverview.txt" supposedFile="supposed-out/stringJoinMapOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringLeft" graphFile="graph/stringLeft.grf">
	 <FlatFile outputFile="data-out/stringLeftOverview.txt" supposedFile="supposed-out/stringLeftOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringLength" graphFile="graph/stringLength.grf">
	 <FlatFile outputFile="data-out/stringLengthOverview.txt" supposedFile="supposed-out/stringLengthOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringLowerCase" graphFile="graph/stringLowerCase.grf">
	 <FlatFile outputFile="data-out/stringLowerCaseOverview.txt" supposedFile="supposed-out/stringLowerCaseOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringMatches" graphFile="graph/stringMatches.grf">
	 <FlatFile outputFile="data-out/stringMatchesOverview.txt" supposedFile="supposed-out/stringMatchesOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringMetaphone" graphFile="graph/stringMetaphone.grf">
	 <FlatFile outputFile="data-out/stringMetaphoneOverview.txt" supposedFile="supposed-out/stringMetaphoneOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringNYSIIS" graphFile="graph/stringNYSIIS.grf">
	 <FlatFile outputFile="data-out/stringNYSIISOverview.txt" supposedFile="supposed-out/stringNYSIISOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringRandomString" graphFile="graph/stringRandomString.grf">
	 <FlatFile outputFile="data-out/stringRandomStringOverview.txt" supposedFile="supposed-out/stringRandomStringOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringRemoveBlankSpace" graphFile="graph/stringRemoveBlankSpace.grf">
	 <FlatFile outputFile="data-out/stringRemoveBlankSpaceOverview.txt" supposedFile="supposed-out/stringRemoveBlankSpaceOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringRemoveDiacritic" graphFile="graph/stringRemoveDiacritic.grf">
	 <FlatFile outputFile="data-out/stringRemoveDiacriticOverview.txt" supposedFile="supposed-out/stringRemoveDiacriticOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringRemoveNonAscii" graphFile="graph/stringRemoveNonAscii.grf">
	 <FlatFile outputFile="data-out/stringRemoveNonAsciiOverview.txt" supposedFile="supposed-out/stringRemoveNonAsciiOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringRemoveNonPrintable" graphFile="graph/stringRemoveNonPrintable.grf">
	 <FlatFile outputFile="data-out/stringRemoveNonPrintableOverview.txt" supposedFile="supposed-out/stringRemoveNonPrintableOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringReplace" graphFile="graph/stringReplace.grf">
	 <FlatFile outputFile="data-out/stringReplaceOverview.txt" supposedFile="supposed-out/stringReplaceOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringRight" graphFile="graph/stringRight.grf">
	 <FlatFile outputFile="data-out/stringRightOverview.txt" supposedFile="supposed-out/stringRightOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringSoundex" graphFile="graph/stringSoundex.grf">
	 <FlatFile outputFile="data-out/stringSoundexOverview.txt" supposedFile="supposed-out/stringSoundexOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringSplit" graphFile="graph/stringSplit.grf">
	 <FlatFile outputFile="data-out/stringSplitOverview.txt" supposedFile="supposed-out/stringSplitOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringSubstring" graphFile="graph/stringSubstring.grf">
	 <FlatFile outputFile="data-out/stringSubstringOverview.txt" supposedFile="supposed-out/stringSubstringOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringTranslate" graphFile="graph/stringTranslate.grf">
	 <FlatFile outputFile="data-out/stringTranslateOverview.txt" supposedFile="supposed-out/stringTranslateOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringTrim" graphFile="graph/stringTrim.grf">
	 <FlatFile outputFile="data-out/stringTrimOverview.txt" supposedFile="supposed-out/stringTrimOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringUpperCase" graphFile="graph/stringUpperCase.grf">
	 <FlatFile outputFile="data-out/stringUpperCaseOverview.txt" supposedFile="supposed-out/stringUpperCaseOverview.txt"/>
</FunctionalTest>


<FunctionalTest ident="mathBitAnd" graphFile="graph/mathBitAnd.grf">
	 <FlatFile outputFile="data-out/mathBitAndOverview.txt" supposedFile="supposed-out/mathBitAndOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathBitIsSet" graphFile="graph/mathBitIsSet.grf">
	 <FlatFile outputFile="data-out/mathBitIsSetOverview.txt" supposedFile="supposed-out/mathBitIsSetOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathBitLShift" graphFile="graph/mathBitLShift.grf">
	 <FlatFile outputFile="data-out/mathBitLShiftOverview.txt" supposedFile="supposed-out/mathBitLShiftOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathBitNegate" graphFile="graph/mathBitNegate.grf">
	 <FlatFile outputFile="data-out/mathBitNegateOverview.txt" supposedFile="supposed-out/mathBitNegateOverview.txt"/>	                                                                    
</FunctionalTest>

<FunctionalTest ident="mathBitOr" graphFile="graph/mathBitOr.grf">
	 <FlatFile outputFile="data-out/mathBitOrOverview.txt" supposedFile="supposed-out/mathBitOrOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathBitRShift" graphFile="graph/mathBitRShift.grf">
	 <FlatFile outputFile="data-out/mathBitRShiftOverview.txt" supposedFile="supposed-out/mathBitRShiftOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathBitSet" graphFile="graph/mathBitSet.grf">
	<FlatFile outputFile="data-out/mathBitSetOverview.txt" supposedFile="supposed-out/mathBitSetOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathBitXor" graphFile="graph/mathBitXor.grf">
	 <FlatFile outputFile="data-out/mathBitXorOverview.txt" supposedFile="supposed-out/mathBitXorOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathRandom" graphFile="graph/mathRandom.grf">
	 <FlatFile outputFile="data-out/mathRandom.txt" supposedFile="supposed-out/mathRandom.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathRandomBoolean" graphFile="graph/mathRandomBoolean.grf">
	 <FlatFile outputFile="data-out/mathRandomBooleanOverview.txt" supposedFile="supposed-out/mathRandomBooleanOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathRandomGaussian" graphFile="graph/mathRandomGaussian.grf">
	 <FlatFile outputFile="data-out/mathRandomGaussianOverview.txt" supposedFile="supposed-out/mathRandomGaussianOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathRandomIntegerWithoutRange" graphFile="graph/mathRandomIntegerWithoutRange.grf">
	 <FlatFile outputFile="data-out/mathRandomIntegerWithoutRange.txt" supposedFile="supposed-out/mathRandomIntegerWithoutRange.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathRandomIntegerWithRange" graphFile="graph/mathRandomIntegerWithRange.grf">
	 <FlatFile outputFile="data-out/mathRandomIntegerWithRangeOverview.txt" supposedFile="supposed-out/mathRandomIntegerWithRangeOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathRound" graphFile="graph/mathRound.grf">
	 <FlatFile outputFile="data-out/mathRoundOverview.txt" supposedFile="supposed-out/mathRoundOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathSetRandomSeed" graphFile="graph/mathSetRandomSeed.grf">
	 <FlatFile outputFile="data-out/mathSetRandomSeedOverview.txt" supposedFile="supposed-out/mathSetRandomSeedOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathFloor" graphFile="graph/mathFloor.grf">
	 <FlatFile outputFile="data-out/mathFloorOverview.txt" supposedFile="supposed-out/mathFloorOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathCeil" graphFile="graph/mathCeil.grf">
	 <FlatFile outputFile="data-out/mathCeilOverview.txt" supposedFile="supposed-out/mathCeilOverview.txt"/>
</FunctionalTest>

</TestScenario>
