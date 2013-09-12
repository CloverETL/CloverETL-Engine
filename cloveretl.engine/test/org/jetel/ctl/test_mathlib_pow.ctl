number power1;
number power2;
decimal[] intResult;
decimal[] longResult;
decimal[] doubleResult;
decimal[] decimalResult;

function integer transform() {
	power1=pow(3,1.2);
	power2=pow(-10,-0.3);
	printErr('power(-10,-0.3)='+power2);
	
	integer[] intArgs = [ 2 , 3 ];
	long[] longArgs = [ 2L, 3L ];
	double[] doubleArgs = [ 2.0, 3.0 ];
	decimal[] decimalArgs = [ 2.0D, 3.0D ];
	
	// power of integer argument
	intResult[0] = pow(intArgs[0],intArgs[1]);
	intResult[1] = pow(intArgs[0],longArgs[1]);
	intResult[2] = pow(intArgs[0],doubleArgs[1]);
	intResult[3] = pow(intArgs[0],decimalArgs[1]);
	printErr(intResult);
	
	// power of long argument
	longResult[0] = pow(longArgs[0],intArgs[1]);
	longResult[1] = pow(longArgs[0],longArgs[1]);
	longResult[2] = pow(longArgs[0],doubleArgs[1]);
	longResult[3] = pow(longArgs[0],decimalArgs[1]);
	printErr(longResult);
	
	// power of double argument
	doubleResult[0] = pow(doubleArgs[0],intArgs[1]);
	doubleResult[1] = pow(doubleArgs[0],longArgs[1]);
	doubleResult[2] = pow(doubleArgs[0],doubleArgs[1]);
	doubleResult[3] = pow(doubleArgs[0],decimalArgs[1]);
	printErr(doubleResult);
	
	// power of decimal argument
	decimalResult[0] = pow(decimalArgs[0],intArgs[1]);
	decimalResult[1] = pow(decimalArgs[0],longArgs[1]);
	decimalResult[2] = pow(decimalArgs[0],doubleArgs[1]);
	decimalResult[3] = pow(decimalArgs[0],decimalArgs[1]);
	printErr(decimalResult);
	return 0;
}