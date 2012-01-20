integer[] intList;
integer[] intList2;
string[] stringList = ['first', 'second', 'third'];
string[] stringListCopy;
string[] stringListCopy2;

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
