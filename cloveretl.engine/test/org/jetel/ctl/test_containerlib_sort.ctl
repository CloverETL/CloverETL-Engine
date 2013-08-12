integer[] intList;
integer[] intList2;
long[] longList;
long[] longList2;
decimal[] decList;
decimal[] decList2;
number[] numList;
number[] numList2;
date[] dateList;
date[] dateList2;
string[] strList;
string[] strList2;
string[] emptyList;
string[] emptyList2;

function integer transform() {
	intList = [3, 2, 1, 1, 5];
	intList2 = sort(intList);
	longList = [23l, 21l, 24l, 22l];
	longList2 = longList.sort();
	decList = [1.4d,1.2d,1.1d,1.3d];
	decList2 = sort(decList);
	numList = [1.3,1.2,1.4,1.1];
	numList2 = numList.sort();
	dateList = [str2date('2004-06-12','yyyy-MM-dd'),str2date('2002-06-12','yyyy-MM-dd'),str2date('2003-06-12','yyyy-MM-dd')];
	dateList2 = sort(dateList);
	strList = ['Soraka', 'Nocturne', 'Alistar', ''];
	strList2 = strList.sort();
	emptyList2 = sort(emptyList);
	return 0;
}