date testFieldAccessDate1;
string testFieldAccessString1;
date[] testFieldAccessDateList1;
string[] testFieldAccessStringList1;
map[string, integer] testFieldAccessIntegerMap1;
map[string, string] testFieldAccessStringMap1;
map[string, date] testFieldAccessDateMap1;
firstMultivalueOutput testFieldAccessRecord1;

function integer transform() {

	//---------------- JJTFIELDACCESSEXPRESSION ---------------
	testFieldAccessDate1 = long2date(12000);
	testFieldAccessString1 = "a";
	testFieldAccessDateList1 = [testFieldAccessDate1, testFieldAccessDate1];
	testFieldAccessStringList1 = ["aa", "bb", "cc"];
	testFieldAccessIntegerMap1["first"] = 1;
	testFieldAccessIntegerMap1["second"] = 2;
	testFieldAccessIntegerMap1["third"] = 3;
	testFieldAccessStringMap1["first"] = "aa";
	testFieldAccessStringMap1["second"] = "bb";
	testFieldAccessStringMap1["third"] = "cc";
	testFieldAccessDateMap1["first"] = long2date(12000);
	testFieldAccessDateMap1["second"] = long2date(34000);
	testFieldAccessDateMap1["third"] = long2date(56000);
	testFieldAccessRecord1.dateListField = testFieldAccessDateList1;
	testFieldAccessRecord1.stringListField = testFieldAccessStringList1;
	testFieldAccessRecord1.integerMapField = testFieldAccessIntegerMap1;
	testFieldAccessRecord1.stringMapField = testFieldAccessStringMap1;
	
	$out.firstMultivalueOutput = testFieldAccessRecord1;
	$out.secondMultivalueOutput = $in.multivalueInput;
	$out.thirdMultivalueOutput = $out.secondMultivalueOutput;

	return 0;
}