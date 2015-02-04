integer[] retInt;
integer[] retIntNull;
long[] retLong;
long[] retLongNull;
decimal[] retDec;
decimal[] retDecNull;
number[] retNum;
number[] retNumNull;
string[] retStr;
string[] retStrNull;
boolean[] retBool;
boolean[] retBoolNull;
date[] retDate;
date[] retDateNull;

function integer transform(){
	integer nullInt = null;
	long nullLong = null;
	decimal nullDec = null;
	number nullNum = null;
	string nullStr = null;
	boolean nullBool = null;
	date nullDate = null;
	
//integer
	integer tmpI = 17;
	integer[] intArr = [11, 13, -5];
	retInt[0] = min(intArr);
	retInt[1] = min([18, 59, 14]);
	retInt[2] = min(tmpI, tmpI);
	retInt[3] = min(15, tmpI);
	retInt[4] = min(tmpI, -9);
	retInt[5] = min(-48, -5);
	intArr = [null, 15, -3];
	retIntNull[0] = min(intArr);
	retIntNull[1] = min([-9, 8, null]);
	retIntNull[2] = min([nullInt, nullInt, null]);
	retIntNull[3] = min(null, 19);
	retIntNull[4] = min(12, null);
	retIntNull[5] = min(nullInt, nullInt);
	retIntNull[6] = min(nullInt, 15);
	retIntNull[7] = min(56, nullInt);

//long
	long tmpL = 156L;
	long[] longArr = [-76L, 87L, 11l];
	retLong[0] = min(longArr);
	retLong[1] = min([26l, 86l, 12l]);
	retLong[2] = min(tmpL, tmpL);
	retLong[3] = min(tmpL, -6L);
	retLong[4] = min(76l, tmpL);
	retLong[5] = min(12l, -6l);		
	longArr = [null, 500L, -30L];
	retLongNull[0] = min(longArr);
	retLongNull[1] = min([null, 98L, 12L]);
	retLongNull[2] = min([nullLong, null, null]);
	retLongNull[3] = min([null, 13l]);
	retLongNull[4] = min([12l, null]);
 	retLongNull[5] = min([nullLong, nullLong]);
	retLongNull[6] = min([nullLong, 89L]);
	retLongNull[7] = min([-4L, nullLong]);	
	//mixed - upcast integer -> long
	retLongNull[8] = min(12, 15l);
	retLongNull[9] = min(56L, -9);
 	retLongNull[10] = min(nullInt, -5L);
	retLongNull[11] = min(-89, nullLong);
	
//number
	number tmpN = 56.9;
	number[] numArr = [12.3, 44.1, 0.99];
	retNum[0] = min(numArr);
	retNum[1] = min([34.1, 0.44, -90.1]);
	retNum[2] = min(tmpN, tmpN);
	retNum[3] = min(tmpN, 89.01);
	retNum[4] = min(45.1, tmpN);
	retNum[5] = min(-1.11, 2.22);
	numArr = [null, 11.3, 112.4];
	retNumNull[0] = min(numArr);
	retNumNull[1] = min([12.3, -11.1, null]);
	retNumNull[2] = min([nullNum, null, null]);
	retNumNull[3] = min(nullNum, nullNum);
 	retNumNull[4] = min(null, nullNum);
	retNumNull[5] = min(nullNum, 12.3);
 	retNumNull[6] = min(11.2, nullNum);
	retNumNull[7] = min(null, 11.6);
	retNumNull[8] = min(12.6, null);
	//mixed - upcast integer -> number
	retNumNull[9] = min(12.3, 15);
	retNumNull[10] = min(tmpI, 33.6);
	retNumNull[11] = min(nullInt, 12.6);
	retNumNull[12] = min(12, nullNum);	
	retNumNull[13] = min(nullInt, nullNum);
	//mixed - upcast long -> number
	retNumNull[14] = min(12.3, 899L);
	retNumNull[15] = min(tmpL, -23.6);
	retNumNull[16] = min(nullLong, 36.9);
 	retNumNull[17] = min(23l, nullNum);
	retNumNull[18] = min(nullNum, nullLong);

//decimal
	decimal tmpD = 56.7d;
	decimal[] decArr = [-7.8d, 11.2d, 45.6d];
	retDec[0] = min(decArr);
	retDec[1] = min([54.1d, 12.4d, -1.11d]);
	retDec[2] = min(tmpD, tmpD);
	retDec[3] = min(32.1d, tmpD);
	retDec[4] = min(tmpD, 66.3d);
	retDec[5] = min(-12.4d, 11.3d);
	decArr = [null, 34.2d, 65.2d];
	retDecNull[0] = min(decArr);
	retDecNull[1] = min([12.3d, null, -1.6d]);
	retDecNull[2] = min([nullDec, null, null]);
 	retDecNull[3] = min(nullDec, nullDec);
	retDecNull[4] = min(nullDec, null);
	retDecNull[5] = min(nullDec, 12.3d);
	retDecNull[6] = min(11.2d, nullDec);
	retDecNull[7] = min(null, 1.11d);
 	retDecNull[8] = min(-13.5d, null);
	//mixed - upcast integer -> decimal
	retDecNull[9] = min(2.11d, 11);
	retDecNull[10] = min(tmpI, -23.9d);
	retDecNull[11] = min(nullInt, 89.7d);
	retDecNull[12] = min(nullDec, 16);
	retDecNull[13] = min(nullDec, nullInt);
	//mixed - upcast long -> decimal
	retDecNull[14] = min(23.6d, 567L);
 	retDecNull[15] = min(tmpL, 78.9d);
	retDecNull[16] = min(nullLong, 45.3d);
	retDecNull[17] = min(nullDec, 458l);
	retDecNull[18] = min(nullLong, nullDec);
	//mixed - upcast number -> decimal
	retDecNull[19] = min(36.9d, 100.1); 
	retDecNull[20] = min(tmpN, -89.6d);
	retDecNull[21] = min(nullNum, 78.9d);
	retDecNull[22] = min(nullDec, 0.0); 	
	retDecNull[23] = min(nullNum, nullDec);	

//string
	string[] strArr = ['Karthus', 'Jax', 'Ryze'];
	retStr[0] = min(strArr);
	retStr[1] = min(['Zyra', 'Taric', 'Sivir']);
	strArr = [null, 'Vi', 'Lulu'];
	retStrNull[0] = min(strArr);
	retStrNull[1] = min(['Fizz', null, 'Poppy']);
	retStrNull[3] = min([nullStr, null, null]);

//boolean
	boolean[] boolArr =[true, false, true];
	retBool[0] = min(boolArr);
	retBool[1] = min([true, true, false]);
	boolArr = [null, true, false];
	retBoolNull[0] = min(boolArr);
	retBoolNull[1] = min([true, null, false]);
	retBoolNull[2] = min([nullBool, null, null]);

//date
	date[] dateArr = [2003-11-17, 2012-05-14, 2006-12-13];
	retDate[0] = min(dateArr);
	retDate[1] = min([2011-05-18, 2003-11-17, 2004-05-12]);
	dateArr = [2003-11-17, null, 2014-02-25];
	retDateNull[0] = min(dateArr);
	retDateNull[1] = min([2012-12-13, null, 2003-11-17]);
	retDateNull[2] = min([nullDate, null, null]);
	return 0;
}