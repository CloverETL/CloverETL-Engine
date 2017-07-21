firstInput[] recordList1;
firstInput[] recordList2;
string[] stringList;
integer[] integerList;
long[] longList;
number[] numberList;
boolean[] booleanList;
decimal[] decimalList;
byte[] byteList;


function integer transform() {
	integer i = 3;

	stringList[i] = "test";
	integerList[i] = 8;
	longList[i] = 77;
	numberList[i] = 5.4;
	decimalList[i] = 8.7D;
	booleanList[i] = true;
	byteList[i] = hex2byte("FF");
	
	recordList1[i] = $in.0;
	recordList2[i].Name = "test";
	
	return 0;
}