string input;
decimal lenght1;

integer stringLength;
integer listLength;
integer mapLength;
integer recordLength;

function integer transform() {
	input=' The QUICk !!$  broWn fox juMPS over the lazy DOG ';
	lenght1=length(input);
	printErr('length of '+input+':'+lenght1 );
	
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
	
	return 0;
}