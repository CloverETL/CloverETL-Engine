integer intElem; 
integer intElem1;
integer[] intList;
string strElem;
string strElem2;
string[] strList;
date dateElem;
date dateElem2;
date[] dateList;
byte byteElem;
byte byteElem2;
byte[] byteList;
long longElem;
long longElem2;
long[] longList;
number numElem;
number numElem2;
number[] numList;
decimal decElem;
decimal decElem2;
decimal[] decList;

long emptyElem;
long emptyElem2;
long[] emptyList;
function integer transform() {
	intList = [1,2,3,4,5];
	intElem = poll(intList); 
	intElem1 = intList.poll();
	
	strList = ['Zyra', 'Tresh','Janna', 'Wu Kong'];
	strElem = poll(strList);
	strElem2 = strList.poll();
	
	dateList =[str2date('2002-11-12','yyyy-MM-dd'),str2date('2003-06-12','yyyy-MM-dd'), str2date('2006-10-15','yyyy-MM-dd')];
	dateElem = dateList.poll();
	dateElem2 = poll(dateList);
	
	byteList = [str2byte('Maoki', 'utf-8'), str2byte('Nasus', 'utf-8'), str2byte('Zac','utf-8'), str2byte('Sona','utf-8')];
	byteElem = poll(byteList);
	byteElem2 = byteList.poll();
	
	longList = [12l, 15l, 16l, 23l];
	longElem = poll(longList);
	longElem2 = longList.poll();
	
	numList = [23.6, 15.9, 78.8, 57.2];
	numElem = numList.poll();
	numElem2 = poll(numList);
	
	decList = [12.3d, 23.4d, 34.5d, 45.6d];
	decElem = poll(decList);
	decElem2 = decList.poll();
	
	emptyElem = poll(emptyList);
	emptyElem2 = emptyList.poll();
	return 0;
}