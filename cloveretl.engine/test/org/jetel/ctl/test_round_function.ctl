long[] intResult;
long[] longResult;
long[] doubleResult;
long[] decimalResult;

function integer transform() {
	integer[] intArgs = [ 2 , 3 ];
	long[] longArgs = [ 2L, 3L ];
	double[] doubleArgs = [ 2.4, 3.5 ];
	decimal[] decimalArgs = [ 2.4D, 3.5D ];
	
	// rounding of int argument
	intResult[0] = round(intArgs[0]);
	intResult[1] = round(intArgs[1]);
	print_err(intResult);
	
	// rounding of long argument
	longResult[0] = round(longArgs[0]);
	longResult[1] = round(longArgs[1]);
	print_err(longResult);
	
	// rounding of double argument
	doubleResult[0] = round(doubleArgs[0]);
	doubleResult[1] = round(doubleArgs[1]);
	print_err(doubleResult);
	
	// rounding of decimal argument
	decimalResult[0] = round(decimalArgs[0]);
	decimalResult[1] = round(decimalArgs[1]);
	print_err(decimalResult);
	
	return 0;
}