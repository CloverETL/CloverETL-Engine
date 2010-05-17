integer[] intList;
string[] stringList = ['first', 'second', 'third'];
string[] stringListCopy;

function integer transform() {
	intList[0] = 1; 
	intList[1] = 2;
	intList[2] = intList[0]+intList[1];
	intList[3] = 3; 
	intList[3] = 4;
	stringList[1] = 'replaced';
	stringListCopy = stringList + [ 'fourth' ];
	stringListCopy = stringListCopy + [ 'fifth' ];
	return 0;
}