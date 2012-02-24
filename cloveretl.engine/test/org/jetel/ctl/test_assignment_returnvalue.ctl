string[] stringList1;
string[] stringList2;
string[] stringList3;
firstMultivalueOutput[] recordList1;
map[integer, firstMultivalueOutput] recordMap1;
string[] stringList4;
secondMultivalueOutput record1;
thirdMultivalueOutput record2;
date dictionaryDate1;
string[] testReturnValueDictionary2;
string[] testReturnValue10;
firstMultivalueOutput testReturnValue11;
string[] testReturnValue12;
string[] testReturnValue13;
map[string, integer] integerMap1;


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
	
// test passing by reference of the new lhs value
function integer transform() {

	firstMultivalueOutput tmpReturnValueRecord1;
	
	// identifier
	stringList1 = ["aa", "bb", "cc"];
	(stringList2 = stringList1).clear();
	(stringList3 = ["a"] + ["b"]).clear();

	// array access expression - list
	recordList1[0] = tmpReturnValueRecord1;
	recordList1[0].stringField = "unmodified";
	(recordList1[1] = recordList1[0]).modifyRecord(); 
	
	// array access expression - map
	recordMap1[0] = tmpReturnValueRecord1;
	recordMap1[0].stringField = "unmodified";
	(recordMap1[1] = recordMap1[0]).modifyRecord(); 
	
	// field access expression
	stringList4 = ["aa", "bb", "cc"];
	integerMap1["first"] = 1;
	integerMap1["second"] = 2;
	integerMap1["third"] = 3;
	($out.firstMultivalueOutput.stringListField = stringList4).clear();
	($out.firstMultivalueOutput.integerMapField = integerMap1).clear(); 
	record1.stringField = "unmodified";
	($out.secondMultivalueOutput = record1).modifyRecord();
	record2.stringField = "unmodified";
	($out.thirdMultivalueOutput.* = record2.*).modifyRecord();
	
	// member access expression - dictionary
	dictionaryDate1 = long2date(12000);
	(dictionary.a = dictionaryDate1).trunc();
	testReturnValueDictionary2 = ["aa", "bb", "cc"];
	(dictionary.stringList = testReturnValueDictionary2).clear();
	
	// member access expression - record
	testReturnValue10 = ["aa", "bb", "cc"];
	(testReturnValue11.stringListField = testReturnValue10).clear();
	
	// member access expression - list of records
	testReturnValue12 = ["aa", "bb", "cc"];
	recordList1[2] = tmpReturnValueRecord1;
	(recordList1[2].stringListField = testReturnValue12).clear();
	
	// member access expression - map of records
	testReturnValue13 = ["aa", "bb", "cc"];
	recordMap1[2] = tmpReturnValueRecord1;
	(recordMap1[2].stringListField = testReturnValue13).clear();
	
	
	return 0;
}