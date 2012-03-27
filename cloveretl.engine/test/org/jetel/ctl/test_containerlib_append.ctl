integer appendElem; 
integer[] appendList;

byte[] byteList;
byte appendByte;

function integer transform() {
	appendList = [1,2,3,4,5];
	appendElem = 10;
	append(appendList, appendElem);
	
	byteList = [str2byte("first", "utf-8"), str2byte("second", "utf-8")];
	appendByte = str2byte("third", "utf-8");
	push(byteList, appendByte);
	
	return 0;
}