integer[] retInt;
integer[] retIntNull;
long[] retLong;
long[] retLongNull;
decimal[] retDec;
decimal[] retDecNull;
number[] retNum;
number[] retNumNull;
string[] retString;
string[] retStringNull;
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

	//integer - array
	integer[] tmp1 = [2,4,7,-1,0];
	retInt[0] = max(tmp1);
	tmp1 = [null, 4, 0, null, -1];
	retIntNull[0] = max(tmp1);
	retInt[1] = max([3,1,0,100,545]);
	retIntNull[1] = max([null,-84,-4,-81]);
	tmp1 = [nullInt, null, null];
	retIntNull[2] = max(tmp1);

	//long - array
	long[] tmp2 = [-2L, 10L, 23L];
	retLong[0] = max(tmp2);
	tmp2 = [-2L, null, 40L, -23L];
	retLongNull[0] = max(tmp2);
	retLong[1] = max([10L, 58L, -4L]);
	retLongNull[1] = max([null, 89L, -40L]);
	tmp2 = [nullLong, null, null];
	retLongNull[2] = max(tmp2);
	
	//decimal - array
	decimal[] tmp3 = [56.3D, 12D, -30.76D];
	retDec[0] = max(tmp3);
	tmp3 = [65.9d, null, 12.3d];
	retDecNull[0] = max(tmp3);	
	retDec[1] = max([87.3d, -54.6d, 11.1d]);
	retDecNull[1] = max([-15.2d, null, -12.6d]);
	tmp3 = [nullDec, null, null];
	retDecNull[2] = max(tmp3);

	//number - array
	number[] tmp4 = [12.3, 54.89, 54.889];
	retNum[0] = max(tmp4);
	tmp4 = [-11.5, null, 56.4];
	retNumNull[0] = max(tmp4);
	retNum[1] = max([-110.1, -56.99, 0]);
	retNumNull[1] = max([98.3, 11.3, null]);
	tmp4 = [nullNum, null, null];
	retNumNull[2] = max(tmp4);

	//integer
	integer tmp5 = 11;
	integer tmp6 = 15;
	retInt[2]= max(tmp5, tmp6);
	retInt[3] = max(4, tmp5);
	retInt[4] = max(tmp6, 67);
	retInt[5] = max(-200, -43);
	retIntNull[3] = max(nullInt, nullInt);
	retIntNull[4] = max(12, nullInt);
	retIntNull[5] = max(nullInt, 54);
	retIntNull[6] = max(null,11);
	retIntNull[7] = max(11,null);

	//long
	long tmpL1 = 45L;
	long tmpL2 = -23L;
	retLong[2] = max(tmpL1, tmpL2);
	retLong[3] = max(75L, tmpL1);
	retLong[4] = max(tmpL2, 89L);
	retLong[5] = max(-33L, -11L);
	retLongNull[3] = max(nullLong, nullLong);
 	retLongNull[4] = max(44L, nullLong);
	retLongNull[5] = max(nullLong, 56L);
	retLongNull[6] = max(null, 89L);
  	retLongNull[7] = max(78L, null);
	
	//decimal
	decimal tmpD1 = 65.1d;
	decimal tmpD2 = 45.2d;
	retDec[2] = max(tmpD1, tmpD2);
	retDec[3] = max(tmpD2, 56.3d);
	retDec[4] = max(26.3d, tmpD1);
	retDec[5] = max(-23.9d, -11.1d);
	retDecNull[3] = max(nullDec, nullDec);
  	retDecNull[4] = max(32.4d, nullDec);
	retDecNull[5] = max(nullDec, 21.5d);
	retDecNull[6] = max(null, 11.3d);
  	retDecNull[7] = max(56.3d, null);	

	//number
	number tmpN1 = 23.6;
	number tmpN2 = 89.7;
	retNum[2] = max(tmpN1, tmpN2);
	retNum[3] = max(tmpN2, 87.3);
	retNum[4] = max(11.4, tmpN1);
	retNum[5] = max(-56.9, -12.5);
	retNumNull[3] = max(nullNum, nullNum);
	retNumNull[4] = max(nullNum, 56.9);
	retNumNull[5] = max(45.7, nullNum);
	retNumNull[6] = max(null, -78.6);
	retNumNull[7] = max(-11.2, null);

	//mixed - upcast integer -> long
	retLongNull[8] = max(45, 89L);
	retLongNull[9] = max(78L, 12);	
	retLongNull[10] = max(nullInt, 23L);

	//mixed - upcast integer -> number
	retNumNull[8] = max(78, 23.6);
	retNumNull[9] = max(45.6, 18);
 	retNumNull[10] = max(89.6, tmp5);
	retNumNull[11] = max(nullInt, 56.9);
	
	//mixed - upcast long -> number
	retNumNull[12] = max(56l, 123.3);
	retNumNull[13] = max(45.9, 9l);
 	retNumNull[14] = max(tmpL1, 48.5);
	retNumNull[15] = max(67.8, nullLong);

	//mixed - upcast integer -> decimal
	retDecNull[8] = max(15, 78.1d);
	retDecNull[9] = max(45.3d, 12);
	retDecNull[10] = max(tmp5, 87.9d);
	retDecNull[11] = max(12.3d, nullInt);

	//mixed - upcast long -> decimal
	retDecNull[12] = max(13L, 13.4d);
	retDecNull[13] = max(12.3d, 78L);
	retDecNull[14] = max(nullLong, 59.6d);
 	retDecNull[15] = max(78.6d, tmpL1);

	//mixed - upcast number -> decimal
	retDecNull[16] = max(11.8, 12.3d); 
	retDecNull[17] = max(58.6d, 11.6);
	retDecNull[18] = max(nullNum, 48.6d);
	retDecNull[19] = max(12.5d, tmpD1);

	//string
	string[] strArr = ['Jax', 'Kennen', 'Dr. Mundo'];
	retString[0] = max(strArr);
	retString[1] = max(['Ahri', 'Talon', 'Zac']);
	strArr = ['Anivia', null, 'Warwick'];
	retStringNull[0] = max(strArr);
	retStringNull[1] = max([nullStr, 'Quinn', 'Ezael']);	
	retStringNull[2] = max([nullStr, null, nullStr]);

	//boolean
	boolean[] boolArr = [true, false, true];
	retBool[0] = max(boolArr);
	retBool[1] = max([false, true, false]);
	boolArr = [true, null, false];
	retBoolNull[0] = max(boolArr);
	retBoolNull[1] = max([true, null, false]);
	retBoolNull[2] = max([null, nullBool, nullBool]);
	
	//date
	date[] dateArr = [2011-02-24, 2013-02-24, 2012-02-24];
	retDate[0] = max(dateArr);
	retDate[1] = max([2013-02-24, 2012-02-24, 2010-02-24]);
	dateArr = [null, 2013-02-24, 2010-09-11];
	retDateNull[0] = max(dateArr);
	retDateNull[1] = max([null, 2013-02-24, 2008-05-17]);
	retDateNull[2] = max([nullDate, null, null]);	

	return 0;
}