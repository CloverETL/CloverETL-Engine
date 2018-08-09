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

string strIntVal;
integer intVal1;
map[integer, string] intMap;
map[integer, integer] intMapVal;
string strLongVal;
long longVal1;
map[long, string] longMap;
map[long, long] longMapVal;
string strDecVal;
decimal decVal1;
map[decimal, string] decMap;
map[decimal, decimal] decimalMapVal;
string strNumVal;
number numVal1;
map[number, string] numMap;
map[number, number] numberMapVal;
string strDateVal;
date dateVal1;
map[date, string] dateMap;
map[date, date] dateMapVal;
string strStrVal;
string strVal1;
map[string, string] strMap;
map[string, string] stringMapVal;


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
	
	intMap[1] = "a";
	intMap[2] = "b";
	intMapVal[1] = 1;
	intMapVal[2] = 2;
	strIntVal = intMap.remove(2);
	intVal1 = intMapVal.remove(1);
	
	longMap[1L] = "a";
	longMap[2L] = "b";
	longMapVal[1L] = 1L;
	longMapVal[2L] = 2L;
	strLongVal = longMap.remove(2L);
	longVal1 = longMapVal.remove(1L);
	
	decMap[1.0D] = "a";
	decMap[2.0D] = "b";
	decimalMapVal[1.0d] = 1.0d;
	decimalMapVal[2.0d] = 2.0d;
	strDecVal = decMap.remove(2.0d);
	decVal1 = decimalMapVal.remove(1.0d);
	
	dateMap[str2date('2001-11-13','yyyy-MM-dd')] = "a";
	dateMap[str2date('2002-11-13','yyyy-MM-dd')] = "b";
	dateMapVal[str2date('2001-11-13','yyyy-MM-dd')] = str2date('2001-11-13','yyyy-MM-dd');
	dateMapVal[str2date('2002-11-13','yyyy-MM-dd')] = str2date('2002-11-13','yyyy-MM-dd');
	strDateVal = dateMap.remove(str2date('2002-11-13','yyyy-MM-dd'));
	dateVal1 = dateMapVal.remove(str2date('2001-11-13','yyyy-MM-dd'));

	strMap["1"] = "a";
	strMap["2"] = "b";
	stringMapVal["1"] = "1";
	stringMapVal["2"] = "2";
	strStrVal = strMap.remove("2");
	strVal1 = stringMapVal.remove("1");
	
	return 0;
}