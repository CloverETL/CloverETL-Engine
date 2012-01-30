string[] testReturnValue1;
string[] testReturnValue2;
string[] testReturnValue3;
firstMultivalueOutput[] testReturnValue4;
map[integer, firstMultivalueOutput] testReturnValue5;
string[] testReturnValue6;
secondMultivalueOutput testReturnValue7;
thirdMultivalueOutput testReturnValue8;
date testReturnValueDictionary1;
string[] testReturnValueDictionary2;
string[] testReturnValue10;
firstMultivalueOutput testReturnValue11;
string[] testReturnValue12;
string[] testReturnValue13;


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
	testReturnValue1 = ["aa", "bb", "cc"];
	(testReturnValue2 = testReturnValue1).clear();
	(testReturnValue3 = ["a"] + ["b"]).clear();

	// array access expression - list
	testReturnValue4[0] = tmpReturnValueRecord1;
	testReturnValue4[0].stringField = "unmodified";
	(testReturnValue4[1] = testReturnValue4[0]).modifyRecord(); 
	
	// array access expression - map
	testReturnValue5[0] = tmpReturnValueRecord1;
	testReturnValue5[0].stringField = "unmodified";
	(testReturnValue5[1] = testReturnValue5[0]).modifyRecord(); 
	
	// field access expression
	testReturnValue6 = ["aa", "bb", "cc"];
	($out.firstMultivalueOutput.stringListField = testReturnValue6).clear(); 
	testReturnValue7.stringField = "unmodified";
	($out.secondMultivalueOutput = testReturnValue7).modifyRecord();
	testReturnValue8.stringField = "unmodified";
	($out.thirdMultivalueOutput.* = testReturnValue8.*).modifyRecord();
	
	// member access expression - dictionary
	testReturnValueDictionary1 = long2date(12000);
	(dictionary.a = testReturnValueDictionary1).trunc();
	testReturnValueDictionary2 = ["aa", "bb", "cc"];
	(dictionary.stringList = testReturnValueDictionary2).clear();
	
	// member access expression - record
	testReturnValue10 = ["aa", "bb", "cc"];
	(testReturnValue11.stringListField = testReturnValue10).clear();
	
	// member access expression - list of records
	testReturnValue12 = ["aa", "bb", "cc"];
	testReturnValue4[2] = tmpReturnValueRecord1;
	(testReturnValue4[2].stringListField = testReturnValue12).clear();
	
	// member access expression - map of records
	testReturnValue13 = ["aa", "bb", "cc"];
	testReturnValue5[2] = tmpReturnValueRecord1;
	(testReturnValue5[2].stringListField = testReturnValue13).clear();
	
	
	return 0;
}