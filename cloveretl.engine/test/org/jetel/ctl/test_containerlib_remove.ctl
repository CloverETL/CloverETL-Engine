integer intElem; 
integer[] intList;
long longElem;
long[] longList;
decimal decElem;
decimal[] decList;
number numElem;
number[] numList;
date dateElem;
date[] dateList;
string strElem;
string[] strList;

function integer transform() {
	intList = [1,2,3,4,5];
	intElem = remove(intList,1); 
	
	longList = [11l,12l,13l,14l];
	longElem = longList.remove(2);
	
	decList = [11.1d,11.2d,11.3d,11.4d];
	decElem = remove(decList,2);
	
	numList = [11.1,11.2,11.3,11.4];
	numElem = numList.remove(2);
	
	dateList = [str2date('2001-11-13','yyyy-MM-dd'),str2date('2002-11-13','yyyy-MM-dd'),str2date('2003-11-13','yyyy-MM-dd')];
	dateElem = remove(dateList, 1);
	
	strList = ['Annie', 'Lux', 'Shivana'];
	strElem = strList.remove(2);
	
	return 0;
}