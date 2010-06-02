integer insertElem; 
integer insertIndex;
integer[] insertList;

function integer transform() {
	insertList = [1,2,3,4,5];
	insertElem = 7; 
	insertIndex = 3;
	insert(insertList,insertIndex,insertElem);
	return 0;
}