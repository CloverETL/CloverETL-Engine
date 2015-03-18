integer int1 = 1;
integer[] intList1 = [1];
integer[] intList2 = [1, 2];
integer[] intList3 = [1, 2];
map[string, integer] intMap1;
intMap1["intMap1_key1"] = 5;
intMap1["intMap1_key2"] = 8;

integer cnt1;
integer cnt2;
integer cnt3;
integer cnt4;
integer cnt5;
integer cnt6;
integer cnt7;
integer cnt8;
integer cnt9;

long long1 = 1L;
long long2 = 2L;

decimal decimal1 = 1D;
decimal decimal2 = 2D;
decimal decimal3 = 3D;
decimal decimal4 = 4D;

number num1 = 1.0;
number num2 = 2.0;
number num3 = 3.0;

firstInput myRecord1;
firstInput tmpRecord;

firstInput[] recordList1;
map[string, firstInput] recordMap1;

map[string, integer] singleEvaluationTest;
map[string, firstInput] singleEvaluationMap;

integer integerInit = null;
long longInit = null;
number numberInit = null;
decimal decimalInit = null;
long[] listInit1 = null;
map[string, decimal] mapInit1 = null;
firstInput recordInit1;
recordInit1.Name = null;
firstInput recordInit2 = null;
multivalueInput recordInit3;
recordInit3.stringListField = null;
recordInit3.stringMapField = null;
multivalueInput[] recordListInit;

integer counter = 0;

function integer echo(integer i) {
	counter++;
	return i;
}

function string echo(string s) {
	counter++;
	return s;
}

function integer[] echo(integer[] l) {
	counter++;
	return l;
}

function map[string, integer] echo(map[string, integer] m) {
	counter++;
	return m;
}

function map[string, firstInput] echo2(map[string, firstInput] m) {
	counter++;
	return m;
}

function integer transform() {

	int1 *= 2;	
	intList1[0] *= 2;
	intList2[echo(1)] *= 4;
	cnt1 = counter;
	counter = 0;
	
	echo(intList3)[0] *= 2;
	cnt2 = counter;
	counter = 0;
	intMap1["intMap1_key1"] *= 2;
	
	long1 *= 2L;
	long2 *= 3;
	
	decimal1 *= 2D;
	decimal2 *= 2;
	decimal3 *= 2L;
	decimal4 *= 2.5;
	
	num1 *= 2.0;
	num2 *= 2;
	num3 *= 2L;
	
	dictionary.i211 *= 2;
	dictionary.l452 *= 2;
	dictionary.d621 *= 2.5;
	dictionary.n9342 *= 2L;
	dictionary.integerList[0] *= 3;
	dictionary.integerList[echo(1)] *= 5;
	cnt3 = counter;
	counter = 0;
	dictionary.integerMap[echo("key2")] *= 9;
	cnt4 = counter;
	counter = 0;

	// null list element
	dictionary.integerList[2] *= 8;
	// null map value
	dictionary.integerMap["nonExistingKey"] *= 7;

	$out.0.* = $in.0.*;
	$out.0.Age *= 2L;
	$out.0.BornMillisec *= 2;
	$out.0.Value *= 2;
	$out.0.Currency *= 2.0;
	
	myRecord1.* = $in.0.*;
	myRecord1.Age *= 2L;
	myRecord1.BornMillisec *= 2;
	myRecord1.Value *= 2;
	myRecord1.Currency *= 2.0;
	
	recordList1 = [tmpRecord, tmpRecord];
	recordList1[0].* = $in.0.*;
	recordList1[echo(0)].Age *= 2L;
	cnt5 = counter;
	counter = 0;
	recordList1[0].BornMillisec *= 2;
	recordList1[0].Value *= 2;
	recordList1[0].Currency *= 2.0;
	
	recordMap1["key"] = tmpRecord;
	recordMap1["key"].* = $in.0.*;
	recordMap1[echo("key")].Age *= 2L;
	cnt6 = counter;
	counter = 0;
	recordMap1["key"].BornMillisec *= 2;
	recordMap1["key"].Value *= 2;
	recordMap1["key"].Currency *= 2.0;
	
	singleEvaluationTest["key"] = 222;
	integer singleEvaluationTestResult = echo(singleEvaluationTest)[echo("key")] *= 555;
	cnt7 = counter;
	counter = 0;
	
	singleEvaluationMap["key"] = tmpRecord;
	singleEvaluationMap["key"].Value = 654;
	singleEvaluationTestResult = echo2(singleEvaluationMap)[echo("key")].Value *= 123;
	cnt8 = counter;
	counter = 0;
	
	$out.firstMultivalueOutput.integerListField = [654];
	echo($out.firstMultivalueOutput.integerListField)[echo(0)] *= 111;
	cnt9 = counter;
	counter = 0;
	
	integerInit *= 5;
	longInit *= 77L;
	numberInit *= 5.4;
	decimalInit *= 7.8D;
	listInit1[2] *= 12;
	mapInit1["key"] *= 987D;
	recordInit1.Age *= 12.34;
	//recordInit2.Age *= 12.34; // does not work even with regular assignment
	recordInit3.integerListField[2] *= 42;
	recordInit3.decimalMapField["key"] *= 88.8D;
	
	integer[] emptyList1;
	multivalueInput tmpMultivalueRecord;
	recordListInit[2] = tmpMultivalueRecord; // should not be necessary, but recursive initialization does not work
	recordListInit[2].integerListField = emptyList1; // should not be necessary
	recordListInit[2].integerListField[2] *= 24;
	
	dictionary.i *= 5;
	dictionary.l *= 77L;
	dictionary.d *= 7.8D;
	dictionary.n *= 5.4;
	
	$out.1.* = null;
	$out.1.Age *= 2L;
	$out.1.BornMillisec *= 2;
	$out.1.Value *= 2;
	$out.1.Currency *= 2.0;
	
	$out.2.* = null;
	$out.2.Age = null;
	$out.2.Age *= 2L;
	$out.2.BornMillisec = null;
	$out.2.BornMillisec *= 2;
	$out.2.Value = null;
	$out.2.Value *= 2;
	$out.2.Currency = null;
	$out.2.Currency *= 2.0;
	
	return 0;
}