double ceil1;

double[] intResult;
double[] longResult;
double[] doubleResult;
decimal[] decimalResult;

function integer transform() {
	ceil1=ceil(-pow(3,1.2));
	
	integer[] intArgs = [ 2 , 3 ];
	long[] longArgs = [ 2L, 3L ];
	double[] doubleArgs = [ 2.01, -3.99 ];
	decimal[] decimalArgs = [ 2.01D, -3.99D ];
	
	// ceiling of int argument
	intResult[0] = ceil(intArgs[0]);
	intResult[1] = ceil(intArgs[1]);
	printErr(intResult);
	
	// ceiling of long argument
	longResult[0] = ceil(longArgs[0]);
	longResult[1] = ceil(longArgs[1]);
	printErr(longResult);
	
	// ceiling of double argument
	doubleResult[0] = ceil(doubleArgs[0]);
	doubleResult[1] = ceil(doubleArgs[1]);
	printErr(doubleResult);
	
	// ceiling of decimal argument
	decimalResult[0] = ceil(decimalArgs[0]);
	decimalResult[1] = ceil(decimalArgs[1]);
	printErr(decimalResult);
	
	//testing of precision autocast
	decimal testVal = ceil(decimalArgs[0]) + 12.123D;
	printErr(testVal);
	
	return 0;
}