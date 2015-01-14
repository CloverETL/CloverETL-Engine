string[] values = ["John", "Johnny", "Little John", "Doe", "Defoe", "Dee", "Jersey", "New York"];

integer[] listResults;
integer listEmptyTest2;
integer listEmptyTest3;
integer listEmptyTest4;
integer integerToLongTest;

integer listTest2;
integer listTest3;
integer listTest8;
integer listTest9;
integer listTest10;
integer listTest11;
integer listTest12;
integer listTest13;
integer listTest14;
integer listTest15;
integer listTest16;
integer listTest17;
integer listTest18;
integer listTest19;
integer listTest20;
integer listTest21;
integer listTest22;
integer listTest23;
integer listTest24;
integer listTest25;

function integer transform() {
	$in.multivalueInput.stringListField.sort();
	for (integer i = 0; i < values.length(); i++) {
		listResults[i] = $in.multivalueInput.stringListField.binarySearch(values[i]);
	}
	
	boolean[] boolList;
	listEmptyTest2 = boolList.binarySearch(true);
	listEmptyTest3 = boolList.binarySearch(false);
	boolList = [true, false].sort();
	listTest2 = boolList.binarySearch(true);
	listTest3 = boolList.binarySearch(false);
	
	date myDate = str2date('2001-11-25','yyyy-MM-dd');

	date[] dateList = [str2date('2001-11-18','yyyy-MM-dd'), myDate];
	listTest8 = dateList.binarySearch(myDate);
	listTest9 = dateList.binarySearch(str2date('2001-11-18','yyyy-MM-dd'));
	listTest10 = dateList.binarySearch(str2date('10-11-2003','dd-MM-yyyy'));

	integer myInt = 57;

	integer[] intList = [45, myInt, 144, 201, 578416];
	listTest11 = intList.binarySearch(myInt);
	listTest12 = intList.binarySearch(45);
	listTest13 = intList.binarySearch(365436);

	long myLong = 5455;

	long[] longList = [54l, 87, myLong, 5457L];
	listTest14 = longList.binarySearch(54L);
	listTest15 = longList.binarySearch(myLong);
	listTest16 = longList.binarySearch(5456L);
	integerToLongTest = longList.binarySearch(87l);

	number myNum = 21.4;

	number[] numList = [15.9, myNum, 22.4, 23.4, 46];
	listTest17 = numList.binarySearch(myNum);
	listTest18 = numList.binarySearch(15.9);
	listTest19 = numList.binarySearch(45.9);

	string myStr = 'ccc';

	string[] strList = ['c', myStr, 'cccc', 'dd', 'xxx'];
	listTest20 = strList.binarySearch('c');
	listTest21 = strList.binarySearch(myStr);
	listTest22 = strList.binarySearch('popocatepetl');

	decimal myDec = 24.45d;

	decimal[] decList = [23.34d, myDec, 47.14d, 54.2d];	
	listTest23 = decList.binarySearch(myDec);
	listTest24 = decList.binarySearch(23.34d);
	listTest25 = decList.binarySearch(89.354d);

	integer[] emptyList;
	listEmptyTest4 = emptyList.binarySearch(15);
	
	return 0;
}