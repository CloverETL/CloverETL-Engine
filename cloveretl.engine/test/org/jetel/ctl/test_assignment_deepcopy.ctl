//---------------- JJTVARIABLEDECLARATION -------------
date testVariableDeclarationDate1 = long2date(12000);
date testVariableDeclarationDate2 = testVariableDeclarationDate1;
byte testVariableDeclarationByte1 = hex2byte("ABCDEF");
byte testVariableDeclarationByte2 = testVariableDeclarationByte1;
	
	
firstOutput[] firstRecordList;
firstOutput[] secondRecordList;

firstMultivalueOutput record1;

firstMultivalueOutput[] recordList1;
firstMultivalueOutput[] recordList2;
firstMultivalueOutput recordInList1;
string[] stringListField1;

date testDate1;
map[integer, firstMultivalueOutput] recordMap1;
map[integer, firstMultivalueOutput] recordMap2;
firstMultivalueOutput recordInMap1;
firstMultivalueOutput recordInMap2;

map[integer, date] dateMap1;
date date1;
date date2;

map[integer, byte] byteMap1;
byte byte1;
byte byte2;

date testFieldAccessDate1;
string testFieldAccessString1;
date[] testFieldAccessDateList1;
string[] testFieldAccessStringList1;
map[string, date] testFieldAccessDateMap1;
map[string, string] testFieldAccessStringMap1;
thirdMultivalueOutput testFieldAccessRecord1;

date testMemberAccessDate1;
byte testMemberAccessByte1;
date[] testMemberAccessDateList1;
byte[] testMemberAccessByteList1;
firstMultivalueOutput testMemberAccessRecord1;
firstMultivalueOutput testMemberAccessRecord2;
firstMultivalueOutput testMemberAccessRecord3;
firstMultivalueOutput[] testMemberAccessRecordList1;
map[integer, firstMultivalueOutput] testMemberAccessRecordMap1;
string[] testMemberAccessStringList1;
date[] testMemberAccessDateList2;
byte[] testMemberAccessByteList2;

string[] testReturnValue1;
string[] testReturnValue2;
string[] testReturnValue3;
firstMultivalueOutput[] testReturnValue4;
map[integer, firstMultivalueOutput] testReturnValue5;

string[] testArrayAccessFunctionCallStringList;
firstMultivalueOutput testArrayAccessFunctionCall;
map[string, firstMultivalueOutput] function_call_original_map;
map[string, firstMultivalueOutput] function_call_copied_map;
firstMultivalueOutput[] function_call_original_list;
firstMultivalueOutput[] function_call_copied_list;

string[] stringListNull;
map[string, string] stringMapNull;

// stupid implementation - poor performance
function string listToString(string[] input) {
	string result = "[";
	for (integer i = 0; i < length(input); i++) {
		if (i > 0) {
			result = result + ", ";
		}
		result = result + input[i];
	}
	result = result + "]";
	
	return result;
}

function void modifyRecord(firstMultivalueOutput rec) {
	rec.stringField = "modified";
}
	
function integer transform() {

	firstOutput myRecord;
	
	firstRecordList[0] = myRecord;
	
	firstRecordList[0].Name = "before";
	
	secondRecordList = firstRecordList; // makes a deep copy

	firstRecordList[0].Name = "after";
	
	// --------------- JJTIDENTIFIER ---------------
	record1 = $in.3;
	record1.stringListField[0] = "first";
	$out.4 = record1;
	record1.stringListField[1] = "second";
	
	// record1 == ["first", "second"]
	// $in.3 == []
	// $out.4 == ["first"]
	
	// --------------- JJTARRAYACCESSEXPRESSION - List ---------------
	stringListField1 = ["a", "b", "c"];
	recordInList1.stringListField = stringListField1;
	recordList1[0] = recordInList1;
	recordList2 = recordList1;
	
	// --------------- JJTARRAYACCESSEXPRESSION - Map ---------------
	testDate1 = today();
	recordInMap1.dateField = testDate1;
	recordMap1[0] = recordInMap1; 
	recordInMap2 = recordMap1[0];
	recordMap2 = recordMap1;
	
	date1 = long2date(3000);
	dateMap1[0] = date1;
	
	dateMap1[1] = long2date(4000);
	date2 = dateMap1[1];
	
	byte1 = hex2byte("ABC");
	byteMap1[0] = byte1;

	byteMap1[1] = hex2byte("DEF");
	byte2 = byteMap1[1];
	
	// ------------- JJTARRAYACCESSEXPRESSION - Function call -------
	testArrayAccessFunctionCallStringList = ["aa", "bb", "cc"];
	testArrayAccessFunctionCall.stringListField = testArrayAccessFunctionCallStringList;
	
	function_call_original_map["1"] = testArrayAccessFunctionCall;
	copy(function_call_copied_map, function_call_original_map)["2"] = testArrayAccessFunctionCall;
	function_call_original_list[1] = testArrayAccessFunctionCall;
	copy(function_call_copied_list, function_call_original_list)[2] = testArrayAccessFunctionCall;

	//---------------- JJTFIELDACCESSEXPRESSION ---------------
	testFieldAccessDate1 = long2date(12000);
	testFieldAccessString1 = "a";
	testFieldAccessDateList1 = [testFieldAccessDate1, testFieldAccessDate1];
	testFieldAccessStringList1 = ["aa", "bb", "cc"];
	testFieldAccessDateMap1["first"] = testFieldAccessDate1;
	testFieldAccessDateMap1["second"] = long2date(34000);
	testFieldAccessStringMap1["first"] = "aa";
	testFieldAccessStringMap1["second"] = "bb";
	testFieldAccessStringMap1["third"] = "cc";
	testFieldAccessRecord1.dateListField = testFieldAccessDateList1;
	testFieldAccessRecord1.stringListField = testFieldAccessStringList1;
	testFieldAccessRecord1.dateMapField = testFieldAccessDateMap1;
	testFieldAccessRecord1.stringMapField = testFieldAccessStringMap1;
	
	$out.firstMultivalueOutput.dateField = testFieldAccessDate1;
	$out.firstMultivalueOutput.dateListField[0] = testFieldAccessDate1;
	$out.firstMultivalueOutput.stringListField[0] = testFieldAccessString1;
	$out.firstMultivalueOutput.dateMapField["first"] = testFieldAccessDate1;
	$out.firstMultivalueOutput.stringMapField["first"] = testFieldAccessString1;
	$out.secondMultivalueOutput.dateListField = testFieldAccessDateList1;
	$out.secondMultivalueOutput.stringListField = testFieldAccessStringList1;
	$out.secondMultivalueOutput.dateMapField = testFieldAccessDateMap1;
	$out.secondMultivalueOutput.stringMapField = testFieldAccessStringMap1;
	$out.thirdMultivalueOutput = testFieldAccessRecord1;

	//---------------- JJTMEMBERACCESSEXPRESSION - record ---------------
	testMemberAccessDate1 = long2date(12000);
	testMemberAccessByte1 = hex2byte("ABC");
	testMemberAccessDateList1 = [testMemberAccessDate1, testMemberAccessDate1];
	testMemberAccessByteList1 = [hex2byte("AB"), hex2byte("CD"), hex2byte("EF")];
	
	date[] tmpDateList1;
	byte[] tmpByteList1;
	testMemberAccessRecord1.dateListField = tmpDateList1;
	testMemberAccessRecord1.byteListField = tmpByteList1;

	testMemberAccessRecord1.dateField = testMemberAccessDate1;
	testMemberAccessRecord1.byteField = testMemberAccessByte1;
	testMemberAccessRecord1.dateListField[0] = testMemberAccessDate1;
	testMemberAccessRecord1.byteListField[0] = testMemberAccessByte1;
	testMemberAccessRecord2.dateListField = testMemberAccessDateList1;
	testMemberAccessRecord2.byteListField = testMemberAccessByteList1;
	testMemberAccessRecord3.* = testMemberAccessRecord2.*;
	
	//---------------- JJTMEMBERACCESSEXPRESSION - dictionary -----------
	dictionary.a = testMemberAccessDate1;
	dictionary.y = testMemberAccessByte1;
	
	testMemberAccessStringList1 = [null, "xx"]; 
	dictionary.stringList = testMemberAccessStringList1;
	testMemberAccessDateList2 = [long2date(98000), null, long2date(76000)]; 
	dictionary.dateList = testMemberAccessDateList2;
	testMemberAccessByteList2 = [hex2byte("ABCD"), null, hex2byte("EF")];
	dictionary.byteList = testMemberAccessByteList2;
	
	//---------------- JJTMEMBERACCESSEXPRESSION - array access ---------
	firstMultivalueOutput tmpMemberAccessRecord1;
	tmpMemberAccessRecord1.dateListField = tmpDateList1;
	tmpMemberAccessRecord1.byteListField = tmpByteList1;
	testMemberAccessRecordList1[0] = tmpMemberAccessRecord1;
	testMemberAccessRecordList1[1] = tmpMemberAccessRecord1;
	testMemberAccessRecordMap1[0] = tmpMemberAccessRecord1;
	testMemberAccessRecordMap1[1] = tmpMemberAccessRecord1;
	
	// list
	testMemberAccessRecordList1[0].dateField = testMemberAccessDate1; 
	testMemberAccessRecordList1[0].byteField = testMemberAccessByte1;
	testMemberAccessRecordList1[0].dateListField[0] = testMemberAccessDate1;
	testMemberAccessRecordList1[0].byteListField[0] = testMemberAccessByte1;
	testMemberAccessRecordList1[1].dateListField = testMemberAccessDateList1;
	testMemberAccessRecordList1[1].byteListField = testMemberAccessByteList1;
	testMemberAccessRecordList1[2].* = testMemberAccessRecordList1[1].*;
	
	// map
	testMemberAccessRecordMap1[0].dateField = testMemberAccessDate1; 
	testMemberAccessRecordMap1[0].byteField = testMemberAccessByte1;
	testMemberAccessRecordMap1[0].dateListField[0] = testMemberAccessDate1;
	testMemberAccessRecordMap1[0].byteListField[0] = testMemberAccessByte1;
	testMemberAccessRecordMap1[1].dateListField = testMemberAccessDateList1;
	testMemberAccessRecordMap1[1].byteListField = testMemberAccessByteList1;
	testMemberAccessRecordMap1[2].* = testMemberAccessRecordMap1[1].*;
	
	// passing by reference of the new lhs value
	testReturnValue1 = ["aa", "bb", "cc"];
	(testReturnValue2 = testReturnValue1).clear();
	(testReturnValue3 = ["a"] + ["b"]).clear();
	
	firstMultivalueOutput tmpReturnValueRecord1;
	
	testReturnValue4[0] = tmpReturnValueRecord1;
	testReturnValue4[0].stringField = "unmodified";
	(testReturnValue4[1] = testReturnValue4[0]).modifyRecord(); 
	
	testReturnValue5[0] = tmpReturnValueRecord1;
	testReturnValue5[0].stringField = "unmodified";
	(testReturnValue5[1] = testReturnValue5[0]).modifyRecord(); 
	
	// CLO-1210
	stringListNull[0] = null; 
	stringMapNull["a"] = null;
	
	return 0;
}