integer intElem;
integer intElem2;
integer[] intList;
long longElem;
long longElem2;
long[] longList;
number numElem;
number numElem2;
number[] numList;
decimal decElem;
decimal decElem2;
decimal[] decList;
date dateElem;
date dateElem2;
date[] dateList;
string strElem;
string strElem2;
string[] strList;
string emptyElem;
string emptyElem2;
string[] emptyList;

function integer transform() {
	intList = [1,2,3,4,5];
	intElem = pop(intList);
	intElem2 = intList.pop(); 
	
	longList = [11l,12l,13l,14l];
	longElem = pop(longList);
	longElem2 = longList.pop();
	
	numList =[11.2, 11.3, 11.4, 11.5];
	numElem = pop(numList);
	numElem2 = numList.pop();
	
	decList = [22.2d,22.3d,22.4d,22.5d];
	decElem = pop(decList);
	decElem2 = decList.pop();
	
	dateList =[str2date('2010-06-11','yyyy-MM-dd'), str2date('2011-04-03','yyyy-MM-dd'), str2date('2001-07-13','yyyy-MM-dd'), str2date('2005-09-24','yyyy-MM-dd')];	
	dateElem = pop(dateList);
	dateElem2 = dateList.pop();
	
	strList = ['Kha-Zix', 'Xerath', null, 'Ezrael'];
	strElem = pop(strList);
	strElem2 = strList.pop();
	
	emptyElem = pop(emptyList);
	emptyElem2 = emptyList.pop();
	return 0;
}