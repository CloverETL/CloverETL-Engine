integer appendElem; 
integer[] appendList;

function integer transform() {
	appendList = [1,2,3,4,5];
	appendElem = 10;
	append(appendList, appendElem);
	return 0;
}