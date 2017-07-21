long round1;

long[] intResult;
long[] longResult;
long[] doubleResult;
decimal[] decimalResult;

decimal[] decimal2Result;
number intWithPrecisionResult;
number longWithPrecisionResult;
number ret1;
number ret2;

function integer transform() {
	round1=round(-pow(3,1.2));
	
	integer[] intArgs = [ 2 , 3 ];
	long[] longArgs = [ 2L, 3L ];
	double[] doubleArgs = [ 2.4, 3.5 ];
	decimal[] decimalArgs = [ 2.4D, 3.5D ];
	decimal decimal2Arg = 1234567.1234567D;
	
	// rounding of int argument
	intResult[0] = round(intArgs[0]);
	intResult[1] = round(intArgs[1]);
	
	// rounding of long argument
	longResult[0] = round(longArgs[0]);
	longResult[1] = round(longArgs[1]);
	
	// rounding of double argument
	doubleResult[0] = round(doubleArgs[0]);
	doubleResult[1] = round(doubleArgs[1]);
	
	// rounding of decimal argument
	decimalResult[0] = round(decimalArgs[0]);
	decimalResult[1] = round(decimalArgs[1]);

 	// rounding decimals with precision
	//CLO-1835
	for (integer i = -7; i < 8; i++) {
		decimal2Result.push(round(decimal2Arg, i));
	}
	
	//CLO-1832
	intWithPrecisionResult = round(1234, 2);
	longWithPrecisionResult = round(123456L, 2);
	ret1 = round(1234, -2);
	ret2 = round(13565L, 2);
	return 0;
}