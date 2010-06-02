integer[] intList;
string[] stringList = ['first', 'second', 'third'];
string[] stringListCopy;

function void changeList(string[] lst) {
	lst.push( "extra" );	
}

function integer transform() {
	intList[0] = 1; 
	intList[1] = 2;
	intList[2] = intList[0]+intList[1];
	intList[3] = 3; 
	intList[3] = 4;
	stringListCopy = stringList;
	stringList[1] = 'replaced';
	push(stringList, 'fourth');
	stringListCopy.push('fifth');
	
	changeList(stringList);
	printErr(stringList);
	printErr(stringListCopy);
	return 0;
}
