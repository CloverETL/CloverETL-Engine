integer[] intCopy;
integer[] intRet;
long[] longCopy;
long[] longRet;
number[] numCopy;
number[] numRet;
decimal[] decCopy;
decimal[] decRet;
firstInput[] recordList;
string[] strCopy;
string[] strRet;
date[] dateCopy;
date[] dateRet;
string[] emptyCopy;
string[] emptyRet;

function integer transform() {
	
	intCopy = [1,2];
	intRet = push(intCopy,3);
	
	longCopy  = [12l,13l];
	longRet = longCopy.push(14L);
	
	numCopy = [11.1, 11.2];
	numRet = push(numCopy, 11.3);
	
	decCopy = [12.2d,12.3d];
	decRet =decCopy.push(12.4d);
	
	strCopy = ['Fiora', 'Nunu'];
	strRet = push(strCopy, 'Amumu');
	
	dateCopy = [str2date('2001-06-09','yyyy-MM-dd'),str2date('2005-06-09','yyyy-MM-dd')];
	dateRet = dateCopy.push(str2date('2011-06-09','yyyy-MM-dd'));
	string str = null;
	emptyRet = push(emptyCopy,str);
	
	firstOutput record1;
	secondInput record2;
	recordList = [record1, record2];
	firstInput pushRecord;
	push(recordList, pushRecord);
	
	return 0;
}