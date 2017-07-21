integer lengthByte;
integer lengthByte2;
integer recordLength;
integer recordLength2;
integer listLength;
integer listLength2;
integer mapLength;
integer mapLength2;
integer emptyListLength;
integer emptyListLength2;
integer emptyMapLength;
integer emptyMapLength2;
integer nullLength1;
integer nullLength2;
integer nullLength3;
integer nullLength4;
integer nullLength5;
integer nullLength6;

function integer transform(){

	lengthByte = length($firstInput.ByteArray);
	lengthByte2 = $firstInput.ByteArray.length();
	
	firstInput myRecord;
	recordLength = length(myRecord);
	recordLength2 = myRecord.length();
	
	integer[] listInt = [11,12,13];
	listLength = length(listInt);
	listLength2 = listInt.length();
	
	map[integer, integer] fullMap;
	fullMap[1] = 5;
	fullMap[2] = 7;
	mapLength = fullMap.length();
	mapLength2 = length(fullMap);
	integer[] arr;
	emptyListLength = arr.length();
	emptyListLength2 = length(arr);
	map[string, string] eMap;
	emptyMapLength = eMap.length();
	emptyMapLength2 = length(eMap);
	
	myRecord = null;
	nullLength1 = length(myRecord);
	nullLength2 = myRecord.length();
	
	listInt = null;
	nullLength3 = length(listInt);
	nullLength4 = listInt.length();
	
	fullMap = null;
	nullLength5 = length(fullMap);
	nullLength6 = fullMap.length();
	return 0;
}