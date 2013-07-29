integer[] integerList;
string[] strList;
boolean[] boolList;
byte[] byteList;
long[] longList;
date[] dateList;
number[] numList;
decimal[] decList;
integer[] emptyList;
function integer transform() {
	integerList = [1,2,3,4,5];
	clear(integerList);
	strList = ['a','','b','vv'];
	clear(strList);
	boolList = [true,true,false];
	clear(boolList);
	byteList = [str2byte('month','utf-8'),str2byte('day','utf-16')];
	clear(byteList);
	longList = [817L,772L,99l];
	clear(longList);
	dateList = [str2date('2005-14-11','yyyy-dd-MM')];
	clear(dateList);
	numList = [2.3, 56.9];
	clear(numList);
	decList = [452.5d, 9212,879d];
	clear(decList);
	clear(emptyList);
	return 0;
}