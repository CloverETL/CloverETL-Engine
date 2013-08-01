integer[] intList;
integer[] intList2;
long[] longList;
long[] longList2;
number[] numList;
number[] numList2;
decimal[] decList;
decimal[] decList2;
string[] strList;
string[] strList2;
date[] dateList;
date[] dateList2;
string[] emptyList;
string[] emptyList2;

function integer transform() {
	intList = [1,2,3,4,5];
	intList2 = reverse(intList);
	longList = [11l,12l,13l,14l];
	longList2 = longList.reverse();
	numList = [1.1,1.2,1.3];
	numList2 = reverse(numList);
	decList = [1.1d,1.2d,1.3d];
	decList2 = decList.reverse();
	strList = ['Kog Maw', 'Lulu', null];
	strList2 = reverse(strList);
	dateList = [str2date('2001-03-01','yyyy-MM-dd'),str2date('2002-03-01','yyyy-MM-dd')];
	dateList2 = dateList.reverse();
	emptyList2 = emptyList.reverse();
	return 0;
}