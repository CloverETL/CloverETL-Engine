integer incrementCounter = 0;
string incrementTest;
string[] incrementTestList;

function integer transform() {
	incrementTest = incrementTestList[incrementCounter++] = "newValue";
	
	return 0;
}