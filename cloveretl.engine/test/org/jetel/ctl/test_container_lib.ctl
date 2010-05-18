integer[] origList;
integer[] copyList;
integer popElem; 
integer[] popList;
integer pollElem; 
integer[] pollList;
integer pushElem;
integer[] pushList;
integer insertElem; 
integer insertIndex;
integer[] insertList;
integer removeIndex;
integer removeElem; 
integer[] removeList;
integer[] sortList;
integer[] reverseList;
integer[] removeAllList;

function integer transform() {
	origList = [1,2,3,4,5];
	//copy
	copy(copyList,origList); 
	// pop
	popElem = pop(origList); 
	copy(popList,origList);
	// poll
	pollElem = poll(origList); 
	copy(pollList,origList); 
	// push
	pushElem = 6;
	push(origList,pushElem); 
	copy(pushList,origList);
	// insert
	insertElem = 7; 
	insertIndex = 1;
	insert(origList,insertIndex,insertElem);
	copy(insertList,origList);
	// remove
	removeIndex = 2;
	removeElem = remove(origList,removeIndex); 
	copy(removeList,origList);
	// sort
	sort(origList);
	copy(sortList,origList);
	// reverse
	reverse(origList);
	copy(reverseList,origList);
	remove_all(origList);
	copy(removeAllList,origList);
	return 0;
}