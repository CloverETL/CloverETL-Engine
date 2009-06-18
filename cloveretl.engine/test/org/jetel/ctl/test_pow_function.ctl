function int transform() {
	int[] intArgs = [ 2 , 3 ];
	long[] longArgs = [ 2L, 3L ];
	double[] doubleArgs = [ 2.0, 3.0 ];
	decimal[] decimalArgs = [ 2.0D, 3.0D ];
	
	// power of integer argument
	double[] intResult;
	intResult[0] = pow(intArgs[0],intArgs[1]);
	intResult[1] = pow(intArgs[0],longArgs[1]);
	intResult[2] = pow(intArgs[0],doubleArgs[1]);
	intResult[3] = pow(intArgs[0],decimalArgs[1]);
	print_err(intResult);
	
	// power of long argument
	double[] longResult;
	longResult[0] = pow(longArgs[0],intArgs[1]);
	longResult[1] = pow(longArgs[0],longArgs[1]);
	longResult[2] = pow(longArgs[0],doubleArgs[1]);
	longResult[3] = pow(longArgs[0],decimalArgs[1]);
	print_err(longResult);
	
	// power of double argument
	double[] doubleResult;
	doubleResult[0] = pow(doubleArgs[0],intArgs[1]);
	doubleResult[1] = pow(doubleArgs[0],longArgs[1]);
	doubleResult[2] = pow(doubleArgs[0],doubleArgs[1]);
	doubleResult[3] = pow(doubleArgs[0],decimalArgs[1]);
	print_err(doubleResult);
	
	// power of decimal argument
	double[] decimalResult;
	decimalResult[0] = pow(decimalArgs[0],intArgs[1]);
	decimalResult[1] = pow(decimalArgs[0],longArgs[1]);
	decimalResult[2] = pow(decimalArgs[0],doubleArgs[1]);
	decimalResult[3] = pow(decimalArgs[0],decimalArgs[1]);
	print_err(decimalResult);
	
	return 0;
}