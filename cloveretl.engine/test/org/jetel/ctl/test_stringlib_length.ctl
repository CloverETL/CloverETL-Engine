string input;
decimal lenght1;
integer lenghtByte;

integer stringLength;
integer listLength;
integer mapLength;
integer recordLength;

integer length_empty;
integer length_null1;
integer length_null2;
integer length_null3;
integer length_null4;
integer length_null5;
function integer transform() {
	input=' The QUICk !!$  broWn fox juMPS over the lazy DOG ';
	lenght1=length(input);
	printErr('length of '+input+':'+lenght1 );
	
	lenghtByte = length($firstInput.ByteArray);
	
	// string length
	stringLength = length("12345678");
	printErr("string:" + stringLength);
	
	// list length
	listLength = length([1,2,3,4,5,6,7,8]);
	printErr("list: " + listLength);
	
	// map length
	map[string,integer] m;
	m["first"] = 1;
	m["second"] = 2;
	m["third"] = 3;
	mapLength = length(m);
	printErr("map: " + mapLength);
	
	// record length
	firstInput myRecord;
	recordLength = length(myRecord);
	printErr("record: " + recordLength);
	
	length_empty = length('');
	string str = null;
	length_null1 = length(str);
	string[] strArr = null;
	length_null2 = length(strArr);
	m = null;
	length_null3 = length(m);
	byte b = null;
	length_null4 = length(b);	
	myRecord = null;
	length_null5 = length(myRecord);
	return 0;
}