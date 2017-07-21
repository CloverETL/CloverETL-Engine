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
	round1=roundHalfToEven(-pow(3,1.2));
	
	integer[] intArgs = [ 2 , 3 ];
	long[] longArgs = [ 2L, 3L ];
	double[] doubleArgs = [ 2.4, 2.5, 3.5 ];
	decimal[] decimalArgs = [ 2.4D, 2.5D, 3.5D, 4.5D, 4.6D ];
	decimal decimal2Arg = 1234567.1234567D;
	double double2Arg = 1234567.1234567;
	
	// rounding of int argument
	intResult[0] = roundHalfToEven(intArgs[0]);
	intResult[1] = roundHalfToEven(intArgs[1]);
	
	// rounding of long argument
	longResult[0] = roundHalfToEven(longArgs[0]);
	longResult[1] = roundHalfToEven(longArgs[1]);
	
	// rounding of double argument
	doubleResult[0] = roundHalfToEven(doubleArgs[0]);
	doubleResult[1] = roundHalfToEven(doubleArgs[1]);
	doubleResult[2] = roundHalfToEven(doubleArgs[2]);
	
	// rounding of decimal argument
	decimalResult[0] = roundHalfToEven(decimalArgs[0]);
	decimalResult[1] = roundHalfToEven(decimalArgs[1]);
	decimalResult[2] = roundHalfToEven(decimalArgs[2]);
	decimalResult[3] = roundHalfToEven(decimalArgs[3]);
	decimalResult[4] = roundHalfToEven(decimalArgs[4]);

 	// rounding decimals with precision

	for (integer i = -7; i < 8; i++) {
		decimal2Result.push(roundHalfToEven(decimal2Arg, i));
	}
	for (integer j = -7; j < 8; j++) {
		double2Result.push(roundHalfToEven(double2Arg, j));
	}
	//CLO-1832
	intWithPrecisionResult = roundHalfToEven(1234, -3);
	longWithPrecisionResult = roundHalfToEven(123456L, 2);
	ret1 = roundHalfToEven(1234, -2);
	ret2 = roundHalfToEven(13565L, -4);
	
	decimal3result[0] = roundHalfToEven(44.445D, 2);
	decimal3result[1] = roundHalfToEven(55.555D, 2);
	decimal3result[2] = roundHalfToEven(66.665D, 2);
	decimal3result[3] = roundHalfToEven(44.4445D, 3);
	decimal3result[4] = roundHalfToEven(55.5555D, 3);
	decimal3result[5] = roundHalfToEven(66.6665D, 3);
	decimal3result[6] = roundHalfToEven(444500D, -3);
	decimal3result[7] = roundHalfToEven(555500D, -3);
	decimal3result[8] = roundHalfToEven(666500D, -3);
	decimal3result[9] = roundHalfToEven(4444.5D, 0);
	decimal3result[10] = roundHalfToEven(5555.5D, 0);
	decimal3result[11] = roundHalfToEven(6666.5D, 0);

	return 0;
}