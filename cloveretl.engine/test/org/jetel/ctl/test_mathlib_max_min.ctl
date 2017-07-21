
long intResult;
long longResult;
long doubleResult;
decimal[] decimalResult;

decimal[] decimal2Result;

function integer transform() {

	integer[] intArgs = [ 2 , 3 , 4];
	long[] longArgs = [ 2L, 3L , 4L];
	double[] doubleArgs = [ 2.4, 3.5 , 4.5];
	decimal[] decimalArgs = [ 2.4D, 3.5D, 4.5D ];
	
	
	// max min of array argument
	printErr(max(intArgs));
	printErr(min(decimalArgs));	
	
	
	// max&min of two arguments
	printErr(max(2,4));
	printErr(min(2,4));
	printErr(max(2.4D,4.5D));
	printErr(min(2.4D,4.5D));
	printErr(min(2.4,4.5));
	printErr(max(2.4,4.5));
	
	return 0;
}