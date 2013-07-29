integer[] origIntList;
integer[] copyIntList;
integer[] returnedIntList;

long[] origLongList;
long[] copyLongList;
long[] returnedLongList;

boolean[] origBoolList;
boolean[] copyBoolList;
boolean[] returnedBoolList;

date[] origDateList;
date[] copyDateList;
date[] returnedDateList;

string[] origStrList;
string[] copyStrList;
string[] returnedStrList;

number[] origNumList;
number[] copyNumList;
number[] returnedNumList;

decimal[] origDecList;
decimal[] copyDecList;
decimal[] returnedDecList;

map[string, string] origStrMap;
map[string, string] copyStrMap;
map[string, string] returnedStrMap;

map[integer,integer] origIntMap;
map[integer,integer] copyIntMap;
map[integer,integer] returnedIntMap;

map[long, long] origLongMap;
map[long, long] copyLongMap;
map[long, long] returnedLongMap;

map[number, number] origNumMap;
map[number, number] copyNumMap;
map[number, number] returnedNumMap;

map[decimal, decimal] origDecMap;
map[decimal, decimal] copyDecMap;
map[decimal, decimal] returnedDecMap;

function integer transform() {
	origIntList = [1,2,3,4,5];
	returnedIntList = copy(copyIntList,origIntList);
	
	long longVal = null;
	origLongList = [longVal, 10L];
	copyLongList = [21L,15L];
	returnedLongList = copy(copyLongList, origLongList);
	
	boolean boolVal = null;
	origBoolList = [boolVal, true];
	copyBoolList = [false, false];
	returnedBoolList = copy(copyBoolList, origBoolList);
	
	date dateVal = null;
	origDateList = [dateVal, str2date('2006-11-12','yyyy-MM-dd')];
	copyDateList = [str2date('2002-04-12','yyyy-MM-dd')];
	returnedDateList = copy(copyDateList, origDateList);
	
	string strVal = null;
	origStrList = [strVal, 'Rengar'];
	copyStrList = ['Ashe', 'Jax'];
	returnedStrList = copy(copyStrList, origStrList);
	
	number numVal = null;
	origNumList = [numVal, 15.65];
	copyNumList = [12.65, 458.3];
	returnedNumList = copy(copyNumList, origNumList);
	
	decimal decVal = null;
	origDecList = [decVal, 15.3d];
	copyDecList = [2.3d, 5.9d];
	returnedDecList = copy(copyDecList, origDecList);
	
	origStrMap["a"] = "a"; 
	origStrMap["b"] = "b"; 
	origStrMap["c"] = "c"; 
	origStrMap["d"] = "d";
	returnedStrMap = copy(copyStrMap, origStrMap);
	
	integer intVal = null; 
	origIntMap[1] = 12;
	origIntMap[2] = intVal;
	copyIntMap[3] = 15;
	returnedIntMap = copy(copyIntMap, origIntMap); 
	
	origLongMap[10L] = 453L;
	origLongMap[11L] = longVal;
	copyLongMap[12L] = 54755L;
	returnedLongMap = copy(copyLongMap, origLongMap); 
	
	origDecMap[2.2d] = 12.3d;
	copyDecMap[2.3d] = 45.6d;
	returnedDecMap = copy(copyDecMap, origDecMap);
	
	origNumMap[12.3] = 11.2;
	copyNumMap[13.4] = 78.9;
	returnedNumMap = copy(copyNumMap, origNumMap);
	
	return 0;
}
