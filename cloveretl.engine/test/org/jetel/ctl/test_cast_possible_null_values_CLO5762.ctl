integer integerResult;
integer integerCurrent;

long longResult;
long longCurrent;

long unspecifiedResult;
long unspecifiedCurrent;

double doubleResult1;
double doubleResult2;
double doubleCurrent1;
double doubleCurrent2;

decimal decimalResult1;
decimal decimalResult2;
decimal decimalResult3;
decimal decimalResult4;
decimal decimalResult5;
decimal decimalResult6;
decimal decimalResult7;

decimal decimalCurrent3;
decimal decimalCurrent4;
decimal decimalCurrent5;
decimal decimalCurrent6;
decimal decimalCurrent7;

function integer transform() {
	
	/* Sequence are used to detect duplicate expression evaluation.
	 * Output ports are used as a temporary variable to force
	 * compiler to use casting.
	 */
	// casts to integer, long
	$out.0.Value = sequence(TestSequence, integer).next();
	integerResult = $out.0.Value;
	integerCurrent = sequence(TestSequence, integer).current(); 
	
	sequence(TestSequence).reset();
	$out.0.BornMillisec = sequence(TestSequence, long).next();
	longResult = $out.0.BornMillisec;  
	longCurrent = sequence(TestSequence, long).current(); 
	
	// use sequence generator without defined type
	sequence(TestSequence).reset();
	$out.0.BornMillisec = sequence(TestSequence).next();
	unspecifiedResult = $out.0.BornMillisec;  
	unspecifiedCurrent = sequence(TestSequence).current(); 
	
	// casts to double
	sequence(TestSequence).reset();
	double x1 = 1.0 - 1.0 + sequence(TestSequence).next();
	$out.0.Age = x1;
	doubleResult1 = $out.0.Age;  
	doubleCurrent1 = sequence(TestSequence).current();
	
	sequence(TestSequence).reset();
	double x2 = sequence(TestSequence).next();
	$out.0.Age = x2;
	doubleResult2 = $out.0.Age; 
	doubleCurrent2 = sequence(TestSequence).current();
	
	// casts to decimal
	$out.0.Currency = 0;
	decimalResult1 = $out.0.Currency;
	
	$out.0.Currency = 2 - 1;
	decimalResult2 = $out.0.Currency;
	
	sequence(TestSequence).reset();
	integer y1 = 3 - 1 + sequence(TestSequence).next();
	$out.0.Currency = y1;
	decimalResult3 = $out.0.Currency;  
	decimalCurrent3 = sequence(TestSequence).current();
	
	sequence(TestSequence).reset();
	double y2 = sequence(TestSequence).next();
	$out.0.Currency = y2;
	decimalResult4 = 3 + $out.0.Currency; 
	decimalCurrent4 = sequence(TestSequence).current();
	
	sequence(TestSequence).reset();
	number y3 = sequence(TestSequence).next();
	$out.0.Currency = y3;
	decimalResult5 = 4 + $out.0.Currency; 
	decimalCurrent5 = sequence(TestSequence).current();
	
	sequence(TestSequence).reset();
	decimal y4 = 5 + sequence(TestSequence).next();
	$out.0.Currency = y4;
	decimalResult6 = $out.0.Currency; 
	decimalCurrent6 = sequence(TestSequence).current();
	
	sequence(TestSequence).reset();
	long y5 = 1 + 5 + sequence(TestSequence).next();
	$out.0.Currency = y5;
	decimalResult7 = $out.0.Currency;  
	decimalCurrent7 = sequence(TestSequence).current();
	
	// print info to console
	printErr('Current integer:     ' + integerCurrent);
	printErr('Current long:        ' + longCurrent);
	printErr('Current unspecified: ' + unspecifiedCurrent);
	printErr('Current double(1):   ' + doubleCurrent1);
	printErr('Current double(2):   ' + doubleCurrent2);
	printErr('Current decimal(3):  ' + decimalCurrent3);
	printErr('Current decimal(4):  ' + decimalCurrent4);
	printErr('Current decimal(5):  ' + decimalCurrent5);	
	printErr('Current decimal(6):  ' + decimalCurrent6);
	printErr('Current decimal(7):  ' + decimalCurrent7);
	
	printErr('Result decimal(1):   ' + decimalResult1);
	printErr('Result decimal(2):   ' + decimalResult2);
	printErr('Result decimal(3):   ' + decimalResult3);
	printErr('Result decimal(4):   ' + decimalResult4);
	printErr('Result decimal(5):   ' + decimalResult5);
	printErr('Result decimal(6):   ' + decimalResult6);
	printErr('Result decimal(7):   ' + decimalResult7);	
	return 0;
}