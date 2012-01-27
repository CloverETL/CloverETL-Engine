date testFieldAccessDate1;
string testFieldAccessString1;
date[] testFieldAccessDateList1;
string[] testFieldAccessStringList1;
firstMultivalueOutput testFieldAccessRecord1;

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

	//---------------- JJTFIELDACCESSEXPRESSION ---------------
	testFieldAccessDate1 = long2date(12000);
	testFieldAccessString1 = "a";
	testFieldAccessDateList1 = [testFieldAccessDate1, testFieldAccessDate1];
	testFieldAccessStringList1 = ["aa", "bb", "cc"];
	testFieldAccessRecord1.dateListField = testFieldAccessDateList1;
	testFieldAccessRecord1.stringListField = testFieldAccessStringList1;
	
	$out.firstMultivalueOutput = testFieldAccessRecord1;
	$out.secondMultivalueOutput = $in.multivalueInput;
	$out.thirdMultivalueOutput = $out.secondMultivalueOutput;

	return 0;
}