<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE TestScenario SYSTEM "testscenario.dtd">

<TestScenario ident="CTLFunctionsTutorial" description="CT LFunctions Tutorial graphs" useJMX="true">    
 
<FunctionalTest ident="containerCopy" graphFile="graph/containerCopy.grf">
	 <FlatFile outputFile="data-out/containerCopyOverview.txt" supposedFile="supposed-out/containerCopyOverview.txt"/>
</FunctionalTest>


<FunctionalTest ident="containerInsert" graphFile="graph/containerInsert.grf">
	 <FlatFile outputFile="data-out/containerInsertOverview.txt" supposedFile="supposed-out/containerInsertOverview.txt"/>	                                                                    
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

<FunctionalTest ident="conversionBool2num1Argument" graphFile="graph/conversionBool2num1Argument.grf">
	 <FlatFile outputFile="data-out/conversionBool2num1ArgumentOverview.txt" supposedFile="supposed-out/conversionBool2num1ArgumentOverview.txt"/>
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

<FunctionalTest ident="conversionHex2byte" graphFile="graph/conversionHex2byte.grf">
	 <FlatFile outputFile="data-out/conversionHex2byteOverview.txt" supposedFile="supposed-out/conversionHex2byteOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionLong2date" graphFile="graph/conversionLong2date.grf">
	 <FlatFile outputFile="data-out/conversionLong2dateOverview.txt" supposedFile="supposed-out/conversionLong2dateOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionLong2pacdecimal" graphFile="graph/conversionLong2pacdecimal.grf">
	 <FlatFile outputFile="data-out/conversionLong2pacdecimalOverview.txt" supposedFile="supposed-out/conversionLong2pacdecimalOverview.txt"/>
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

<FunctionalTest ident="conversionNum2num" graphFile="graph/conversionNum2num.grf" assertion="false">
	<RegEx expression="can&apos;t convert &quot;12.30&quot; to INTEGER" />
	 <FlatFile outputFile="data-out/conversionNum2numIntegerToDecimalOverview.txt" supposedFile="supposed-out/conversionNum2numIntegerToDecimalOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionNum2str_Format" graphFile="graph/conversionNum2str_Format.grf">
	 <FlatFile outputFile="data-out/conversionNum2stringFormatOverview.txt" supposedFile="supposed-out/conversionNum2stringFormatOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionNum2strDECIMAL" graphFile="graph/conversionNum2strDECIMAL.grf">
	 <FlatFile outputFile="data-out/conversionNum2stringDECIMALOverview.txt" supposedFile="supposed-out/conversionNum2stringDECIMALOverview.txt"/>
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

<FunctionalTest ident="conversionPacdecimal2long" graphFile="graph/conversionPacdecimal2long.grf">
	 <FlatFile outputFile="data-out/conversionPacdecimal2longOverview.txt" supposedFile="supposed-out/conversionPacdecimal2longOverview.txt"/>
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

<FunctionalTest ident="conversionStr2num_FormatLocale" graphFile="graph/conversionStr2num_FormatLocale.grf">
	 <FlatFile outputFile="data-out/conversionStr2num_FormatOverview.txt" supposedFile="supposed-out/conversionStr2num_FormatOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionStr2numDECIMAL" graphFile="graph/conversionStr2numDECIMAL.grf">
	 <FlatFile outputFile="data-out/conversionStr2numDECIMALOverview.txt" supposedFile="supposed-out/conversionStr2numDECIMALOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionStr2numINTEGER" graphFile="graph/conversionStr2numINTEGER.grf">
	 <FlatFile outputFile="data-out/conversionStr2numINTEGEROverview.txt" supposedFile="supposed-out/conversionStr2numINTEGEROverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionStr2numLONG" graphFile="graph/conversionStr2numLONG.grf">
	 <FlatFile outputFile="data-out/conversionStr2numLONGOverview.txt" supposedFile="supposed-out/conversionStr2numLONGOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionStr2numNUMBER" graphFile="graph/conversionStr2numNUMBER.grf">
	 <FlatFile outputFile="data-out/conversionStr2numNUMBEROverview.txt" supposedFile="supposed-out/conversionStr2numNUMBEROverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionTo_string" graphFile="graph/conversionTo_string.grf">
	 <FlatFile outputFile="data-out/conversionTo_stringOverview.txt" supposedFile="supposed-out/conversionTo_stringOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionTry_convertFromDateToString" graphFile="graph/conversionTry_convertFromDateToString.grf">
	 <FlatFile outputFile="data-out/conversionTry_convertFromDateToStringOverview.txt" supposedFile="supposed-out/conversionTry_convertFromDateToStringOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionTry_convertFromStringToDate" graphFile="graph/conversionTry_convertFromStringToDate.grf">
	 <FlatFile outputFile="data-out/conversionTry_convertFromStringToDateOverview.txt" supposedFile="supposed-out/conversionTry_convertFromStringToDateOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="conversionTry_convertFromTypeToType" graphFile="graph/conversionTry_convertFromTypeToType.grf">
	 <FlatFile outputFile="data-out/conversionTry_convertTypeToTypeOverview.txt" supposedFile="supposed-out/conversionTry_convertTypeToTypeOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateDateadd" graphFile="graph/dateDateadd.grf">
	 <FlatFile outputFile="data-out/dateDateaddOverview.txt" supposedFile="supposed-out/dateDateaddOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateDatediff" graphFile="graph/dateDatediff.grf">
	 <FlatFile outputFile="data-out/dateDatediffOverview.txt" supposedFile="supposed-out/dateDatediffOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateRandom_dateDateArguments" graphFile="graph/dateRandom_dateDateArguments.grf">
</FunctionalTest>

<FunctionalTest ident="dateRandom_dateStringArguments" graphFile="graph/dateRandom_dateStringArguments.grf">
</FunctionalTest>

<FunctionalTest ident="dateToday" graphFile="graph/dateToday.grf">
</FunctionalTest>


<FunctionalTest ident="dateTrunc_date" graphFile="graph/dateTrunc_date.grf">
	 <FlatFile outputFile="data-out/dataTrunc_dateOverview.txt" supposedFile="supposed-out/dataTrunc_dateOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="dateTrunc" graphFile="graph/dateTrunc.grf">
	 <FlatFile outputFile="data-out/dataTruncOverview.txt" supposedFile="supposed-out/dataTruncOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="miscellaneousIff" graphFile="graph/miscellaneousIff.grf">
	 <FlatFile outputFile="data-out/miscellaneousIffOverview.txt" supposedFile="supposed-out/miscellaneousIffOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="miscellaneousIsnull" graphFile="graph/miscellaneousIsnull.grf">
	 <FlatFile outputFile="data-out/overviewMiscellaneousIsnullOverview.txt" supposedFile="supposed-out/overviewMiscellaneousIsnullOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="miscellaneousNvl" graphFile="graph/miscellaneousNvl.grf">
	 <FlatFile outputFile="data-out/miscellaneousNvlOverview.txt" supposedFile="supposed-out/miscellaneousNvlOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="miscellaneousNvl2" graphFile="graph/miscellaneousNvl2.grf">
	 <FlatFile outputFile="data-out/miscellaneousNvl2Overview.txt" supposedFile="supposed-out/miscellaneousNvl2Overview.txt"/>
</FunctionalTest>

<FunctionalTest ident="miscellaneousPrint_err" graphFile="graph/miscellaneousPrint_err.grf">
	 <FlatFile outputFile="data-out/miscellaneousPrint_errOverview.txt" supposedFile="supposed-out/miscellaneousPrint_errOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="miscellaneousPrint_log" graphFile="graph/miscellaneousPrint_log.grf">
	 <FlatFile outputFile="data-out/miscellaneousPrint_log.txt" supposedFile="supposed-out/miscellaneousPrint_log.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringChar_at" graphFile="graph/stringChar_at.grf">
	 <FlatFile outputFile="data-out/stringChar_atOverview.txt" supposedFile="supposed-out/stringChar_atOverview.txt"/>
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

<FunctionalTest ident="stringCount_char" graphFile="graph/stringCount_char.grf">
	 <FlatFile outputFile="data-out/stringCount_charOverview.txt" supposedFile="supposed-out/stringCount_charOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringCut" graphFile="graph/stringCut.grf">
	 <FlatFile outputFile="data-out/stringCutOverview.txt" supposedFile="supposed-out/stringCutOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringEdit_distanceIDENTICAL" graphFile="graph/stringEdit_distanceIDENTICAL.grf">
	 <FlatFile outputFile="data-out/stringEdit_distanceIDENTICALOverview.txt" supposedFile="supposed-out/stringEdit_distanceIDENTICALOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringEdit_distancePRIMARY" graphFile="graph/stringEdit_distancePRIMARY.grf">
	 <FlatFile outputFile="data-out/stringEdit_distancePRIMARYOverview.txt" supposedFile="supposed-out/stringEdit_distancePRIMARYOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringEdit_distanceSECONDARY" graphFile="graph/stringEdit_distanceSECONDARY.grf">
	 <FlatFile outputFile="data-out/stringEdit_distanceSECONDARYOverview.txt" supposedFile="supposed-out/stringEdit_distanceSECONDARYOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringEdit_distanceTERTIARY" graphFile="graph/stringEdit_distanceTERTIARY.grf">
	 <FlatFile outputFile="data-out/stringEdit_distanceTERTIARYOverview.txt" supposedFile="supposed-out/stringEdit_distanceTERTIARYOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringFind" graphFile="graph/stringFind.grf">
	 <FlatFile outputFile="data-out/stringFindOverview.txt" supposedFile="supposed-out/stringFindOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringGet_alphanumeric_chars" graphFile="graph/stringGet_alphanumeric_chars.grf">
	 <FlatFile outputFile="data-out/stringGet_alphanumeric_chars.txt" supposedFile="supposed-out/stringGet_alphanumeric_chars.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIndex_of" graphFile="graph/stringIndex_of.grf">
	 <FlatFile outputFile="data-out/stringIndex_ofOverview.txt" supposedFile="supposed-out/stringIndex_ofOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIs_ascii" graphFile="graph/stringIs_ascii.grf">
	 <FlatFile outputFile="data-out/stringIs_asciiOverview.txt" supposedFile="supposed-out/stringIs_asciiOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIs_blank" graphFile="graph/stringIs_blank.grf">
	 <FlatFile outputFile="data-out/stringIs_blankOverview.txt" supposedFile="supposed-out/stringIs_blankOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIs_date" graphFile="graph/stringIs_date.grf">
	 <FlatFile outputFile="data-out/stringIs_dateOverview1.txt" supposedFile="supposed-out/stringIs_dateOverview1.txt"/>
	 <FlatFile outputFile="data-out/stringIs_dateOverview2.txt" supposedFile="supposed-out/stringIs_dateOverview2.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIs_integer" graphFile="graph/stringIs_integer.grf">
	 <FlatFile outputFile="data-out/stringIs_integerOverview.txt" supposedFile="supposed-out/stringIs_integerOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIs_long" graphFile="graph/stringIs_long.grf">
	 <FlatFile outputFile="data-out/stringIs_long.txt" supposedFile="supposed-out/stringIs_long.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringIs_number" graphFile="graph/stringIs_number.grf">
	 <FlatFile outputFile="data-out/stringIs_numberOverview.txt" supposedFile="supposed-out/stringIs_numberOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringJoin" graphFile="graph/stringJoin.grf">
	 <FlatFile outputFile="data-out/stringJoinOverview.txt" supposedFile="supposed-out/stringJoinOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringLeft" graphFile="graph/stringLeft.grf">
	 <FlatFile outputFile="data-out/stringLeftOverview.txt" supposedFile="supposed-out/stringLeftOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringLength" graphFile="graph/stringLength.grf">
	 <FlatFile outputFile="data-out/stringLengthOverview.txt" supposedFile="supposed-out/stringLengthOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringLowercase" graphFile="graph/stringLowercase.grf">
	 <FlatFile outputFile="data-out/stringLowercaseOverview.txt" supposedFile="supposed-out/stringLowercaseOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringMetaphone" graphFile="graph/stringMetaphone.grf">
	 <FlatFile outputFile="data-out/stringMetaphoneOverview.txt" supposedFile="supposed-out/stringMetaphoneOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringNYSIIS" graphFile="graph/stringNYSIIS.grf">
	 <FlatFile outputFile="data-out/stringNYSIISOverview.txt" supposedFile="supposed-out/stringNYSIISOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringRandom_string" graphFile="graph/stringRandom_string.grf">
</FunctionalTest>

<FunctionalTest ident="stringRemove_blank_space" graphFile="graph/stringRemove_blank_space.grf">
	 <FlatFile outputFile="data-out/stringRemove_blank_spaceOverview.txt" supposedFile="supposed-out/stringRemove_blank_spaceOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringRemove_diacritic" graphFile="graph/stringRemove_diacritic.grf">
	 <FlatFile outputFile="data-out/stringRemove_diacriticOverview.txt" supposedFile="supposed-out/stringRemove_diacriticOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringRemove_nonascii" graphFile="graph/stringRemove_nonascii.grf">
	 <FlatFile outputFile="data-out/stringRemove_nonasciiOverview.txt" supposedFile="supposed-out/stringRemove_nonasciiOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="stringRemove_nonpritable" graphFile="graph/stringRemove_nonpritable.grf">
	 <FlatFile outputFile="data-out/stringRemove_nonpritableOverview.txt" supposedFile="supposed-out/stringRemove_nonpritableOverview.txt"/>
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

<FunctionalTest ident="stringUppercase" graphFile="graph/stringUppercase.grf">
	 <FlatFile outputFile="data-out/stringUppercaseOverview.txt" supposedFile="supposed-out/stringUppercaseOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathBit_and" graphFile="graph/mathBit_and.grf">
	 <FlatFile outputFile="data-out/mathBit_andOverview.txt" supposedFile="supposed-out/mathBit_andOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathBit_invert" graphFile="graph/mathBit_invert.grf">
	 <FlatFile outputFile="data-out/mathBit_invertOverview.txt" supposedFile="supposed-out/mathBit_invertOverview.txt"/>	                                                                    
</FunctionalTest>


<FunctionalTest ident="mathBit_is_set" graphFile="graph/mathBit_is_set.grf">
	 <FlatFile outputFile="data-out/mathBit_is_setOverview.txt" supposedFile="supposed-out/mathBit_is_setOverview.txt"/>
</FunctionalTest>


<FunctionalTest ident="mathBit_lshift" graphFile="graph/mathBit_lshift.grf">
	 <FlatFile outputFile="data-out/mathBit_lshiftOverview.txt" supposedFile="supposed-out/mathBit_lshiftOverview.txt"/>
</FunctionalTest>


<FunctionalTest ident="mathBit_or" graphFile="graph/mathBit_or.grf">
	 <FlatFile outputFile="data-out/mathBit_orOverview.txt" supposedFile="supposed-out/mathBit_orOverview.txt"/>
</FunctionalTest>


<FunctionalTest ident="mathBit_rshift" graphFile="graph/mathBit_rshift.grf">
	 <FlatFile outputFile="data-out/mathBit_rshiftOverview.txt" supposedFile="supposed-out/mathBit_rshiftOverview.txt"/>
</FunctionalTest>


<FunctionalTest ident="mathBit_set" graphFile="graph/mathBit_set.grf">
	<FlatFile outputFile="data-out/mathBit_setOverview.txt" supposedFile="supposed-out/mathBit_setOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathBit_xor" graphFile="graph/mathBit_xor.grf">
	 <FlatFile outputFile="data-out/mathBit_xorOverview.txt" supposedFile="supposed-out/mathBit_xorOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathRandom_boolean" graphFile="graph/mathRandom_boolean.grf">
	 <FlatFile outputFile="data-out/mathRandom_booleanOverview.txt" supposedFile="supposed-out/mathRandom_booleanOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathRandom_gaussian" graphFile="graph/mathRandom_gaussian.grf">
	 <FlatFile outputFile="data-out/mathRandom_gaussianOverview.txt" supposedFile="supposed-out/mathRandom_gaussianOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathRandom_intWithoutRange" graphFile="graph/mathRandom_intWithoutRange.grf">
	 <FlatFile outputFile="data-out/mathRandom_intWithoutRange.txt" supposedFile="supposed-out/mathRandom_intWithoutRange.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathRandom_intWithRange" graphFile="graph/mathRandom_intWithRange.grf">
	 <FlatFile outputFile="data-out/mathRandom_intWithRangeOverview.txt" supposedFile="supposed-out/mathRandom_intWithRangeOverview.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathRandom_longWithoutRange" graphFile="graph/mathRandom_longWithoutRange.grf">
	 <FlatFile outputFile="data-out/mathRandom_longWithoutRange.txt" supposedFile="supposed-out/mathRandom_longWithoutRange.txt"/>
</FunctionalTest>

<FunctionalTest ident="mathRandom" graphFile="graph/mathRandom.grf">
	 <FlatFile outputFile="data-out/mathRandom.txt" supposedFile="supposed-out/mathRandom.txt"/>
</FunctionalTest>

</TestScenario>
