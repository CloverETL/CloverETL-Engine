integer insertElem; 
integer insertIndex;
integer[] insertList;
integer[] insertList1;
integer[] insertList2;

function integer transform() {
	insertList = [1,2,3,4,5];
	insertElem = 7; 
	insertIndex = 3;
	insert(insertList,insertIndex,insertElem);
	
	insertList1 = [7, 8, 11];
	integer[] toBeInserted = [11, 10];
	insert(insertList1, 2, toBeInserted);
	
	insertList2 = [7, 8, 11];
	integer toBeInserted1 = 10;
	integer toBeInserted2 = 9;
	insert(insertList2, 2, toBeInserted1, toBeInserted2);
	
	
	return 0;
}