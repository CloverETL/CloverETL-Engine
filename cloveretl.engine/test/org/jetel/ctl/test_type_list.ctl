integer[] intList;
integer[] intList2;
string[] stringList = ['first', 'second', 'third'];
string[] stringListCopy;
string[] stringListCopy2;

boolean[] booleanList = [false, null, true];
byte[] byteList = [hex2byte("AB"), null];
cbyte[] cbyteList = [null, hex2byte("CD")];
date[] dateList = [long2date(12000), null, long2date(34000)];
decimal[] decimalList = [null, 12.34d];
integer[] intList3 = [12, null, 34];
long[] longList = [12l, null, 98l];
number[] numberList = [12.34, null, 56.78];
string[] stringList2 = ["aa", null, "bb"];

function void changeList(string[] lst) {
	lst.push( "extra" );	
}

function integer transform() {
	integer integerToAppend = 6;
	string stringToAppend = 'fifth';
	intList[0] = 1; 
	intList[1] = 2;
	intList[2] = intList[0]+intList[1];
	intList[3] = 3; 
	intList[3] = 4;
	intList = intList + [5];
	intList = intList + [integerToAppend];
	
	integer i = 1;
	while(i < 4) {
		intList2 = intList2 + [i];
		i++;
	}
	
	stringList = stringList + ['fourth'];
	stringList = stringList + [stringToAppend];
	stringListCopy = stringList;
	stringList[1] = 'replaced';
	push(stringList, 'sixth');
	stringListCopy.push('seventh');
	
	changeList(stringList);
	
	stringListCopy2 = stringList;
	printErr(stringList);
	printErr(stringListCopy);
	return 0;
}
