double floor1;

double[] intResult;
double[] longResult;
double[] doubleResult;
decimal[] decimalResult;

function integer transform() {
	floor1=floor(-pow(3,1.2));
	
	integer[] intArgs = [ 2 , 3 ];
	long[] longArgs = [ 2L, 3L ];
	double[] doubleArgs = [ 2.1, -3.99 ];
	decimal[] decimalArgs = [ 2.1D, -3.99D ];
	
	// flooring of int argument
	intResult[0] = floor(intArgs[0]);
	intResult[1] = floor(intArgs[1]);
	printErr(intResult);
	
	// flooring of long argument
	longResult[0] = floor(longArgs[0]);
	longResult[1] = floor(longArgs[1]);
	printErr(longResult);
	
	// flooring of double argument
	doubleResult[0] = floor(doubleArgs[0]);
	doubleResult[1] = floor(doubleArgs[1]);
	printErr(doubleResult);
	
	// flooring of decimal argument
	decimalResult[0] = floor(decimalArgs[0]);
	decimalResult[1] = floor(decimalArgs[1]);
	printErr(decimalResult);
	
	return 0;
}