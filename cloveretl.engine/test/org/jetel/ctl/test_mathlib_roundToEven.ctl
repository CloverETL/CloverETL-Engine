decimal round1;

decimal[] intResult;
decimal[] longResult;
decimal[] doubleResult;
decimal[] decimalResult;

decimal[] double2Result;
decimal[] decimal2Result;
decimal intWithPrecisionResult;
decimal longWithPrecisionResult;
decimal ret1;
decimal ret2;

decimal[] decimal3result;

function integer transform() {
	round1=roundToEven(-pow(3,1.2));
	
	integer[] intArgs = [ 2 , 3 ];
	long[] longArgs = [ 2L, 3L ];
	double[] doubleArgs = [ 2.4, 2.5, 3.5 ];
	decimal[] decimalArgs = [ 2.4D, 2.5D, 3.5D, 4.5D, 4.6D ];
	decimal decimal2Arg = 1234567.1234567D;
	double double2Arg = 1234567.1234567;
	
	// rounding of int argument
	intResult[0] = roundToEven(intArgs[0]);
	intResult[1] = roundToEven(intArgs[1]);
	
	// rounding of long argument
	longResult[0] = roundToEven(longArgs[0]);
	longResult[1] = roundToEven(longArgs[1]);
	
	// rounding of double argument
	doubleResult[0] = roundToEven(doubleArgs[0]);
	doubleResult[1] = roundToEven(doubleArgs[1]);
	doubleResult[2] = roundToEven(doubleArgs[2]);
	
	// rounding of decimal argument
	decimalResult[0] = roundToEven(decimalArgs[0]);
	decimalResult[1] = roundToEven(decimalArgs[1]);
	decimalResult[2] = roundToEven(decimalArgs[2]);
	decimalResult[3] = roundToEven(decimalArgs[3]);
	decimalResult[4] = roundToEven(decimalArgs[4]);

 	// rounding decimals with precision

	for (integer i = -7; i < 8; i++) {
		decimal2Result.push(roundToEven(decimal2Arg, i));
	}
	for (integer j = -7; j < 8; j++) {
		double2Result.push(roundToEven(double2Arg, j));
	}
	//CLO-1832
	intWithPrecisionResult = roundToEven(1234, -3);
	longWithPrecisionResult = roundToEven(123456L, 2);
	ret1 = roundToEven(1234, -2);
	ret2 = roundToEven(13565L, -4);
	
	decimal3result[0] = roundToEven(44.445D, 2);
	decimal3result[1] = roundToEven(55.555D, 2);
	decimal3result[2] = roundToEven(66.665D, 2);
	decimal3result[3] = roundToEven(44.4445D, 3);
	decimal3result[4] = roundToEven(55.5555D, 3);
	decimal3result[5] = roundToEven(66.6665D, 3);
	decimal3result[6] = roundToEven(444500D, -3);
	decimal3result[7] = roundToEven(555500D, -3);
	decimal3result[8] = roundToEven(666500D, -3);
	decimal3result[9] = roundToEven(4444.5D, 0);
	decimal3result[10] = roundToEven(5555.5D, 0);
	decimal3result[11] = roundToEven(6666.5D, 0);

	return 0;
}