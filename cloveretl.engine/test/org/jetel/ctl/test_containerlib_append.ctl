integer appendElem; 
integer[] appendList;

byte[] byteList;
byte appendByte;
string[] stringList;
string[] stringList2;
string[] stringList3;

integer[] integerList1;
integer[] integerList2;

number[] numberList1;
number[] numberList2;

long[] longList1;
long[] longList2;

decimal[] decList1;
decimal[] decList2;

function integer transform() {
	appendList = [1,2,3,4,5];
	appendElem = 10;
	append(appendList, appendElem);
	
	byteList = [str2byte("first", "utf-8"), str2byte("second", "utf-8")];
	appendByte = str2byte("third", "utf-8");
	push(byteList, appendByte);
	
	stringList =['horse','is','pretty'];
	append(stringList,'scary');
	string tmp =null;
	stringList2 = ['horse'];
	append(stringList2,tmp);
	stringList3 = ['horse'];
	append(stringList3, '');
	integerList1 = [1,2,3];
	append(integerList1,4);
	integerList2 = [1,2];
	integer var_int = null;
	append(integerList2,var_int);
	numberList1 = [0.21, 1.1];
	append(numberList1,2.2);
	number var_num = null;
	numberList2 = [1.1];
	append(numberList2,var_num);
	longList1 = [1L,2L];
	append(longList1,3L);
	long var_long = null;
	longList2 = [9L];
	append(longList2, var_long);
	decList1 = [2.3d,4.5d];
	append(decList1,6.7d);
	decimal var_dec = null;
	decList2 = [1.1d];
	append(decList2, var_dec);
	return 0;
	
}