integer intPlus;
integer intPlusOrig;
integer intPlusPlus;
integer intMinus;
integer intMinusOrig;
integer intMinusMinus;
long longPlus;
long longPlusOrig;
long longPlusPlus;
long longMinus;
long longMinusOrig;
long longMinusMinus;
number numberPlus;
number numberPlusOrig;
number numberPlusPlus;
number numberMinus;
number numberMinusOrig;
number numberMinusMinus;
decimal decimalPlus;
decimal decimalPlusOrig;
decimal decimalPlusPlus;
decimal decimalMinus;
decimal decimalMinusOrig;
decimal decimalMinusMinus;
integer plusInt;
integer plusIntOrig;
integer plusPlusInt;
integer minusInt;
integer unaryInt;
integer minusIntOrig;
integer minusMinusInt;
long plusLong;
long plusLongOrig;
long plusPlusLong;
long minusLong;
long unaryLong;
long minusLongOrig;
long minusMinusLong;
number plusNumber;
number plusNumberOrig;
number plusPlusNumber;
number minusNumber;
number unaryNumber;
number minusNumberOrig;
number minusMinusNumber;
decimal plusDecimal;
decimal plusDecimalOrig;
decimal plusPlusDecimal;
decimal minusDecimal;
decimal unaryDecimal;
decimal minusDecimalOrig;
decimal minusMinusDecimal;
firstInput plusPlusRecord;
firstInput minusMinusRecord;
firstInput recordPlusPlus;
firstInput recordMinusMinus;
firstInput modifiedPlusPlusRecord;
firstInput modifiedMinusMinusRecord;
firstInput modifiedRecordPlusPlus;
firstInput modifiedRecordMinusMinus;
boolean booleanValue;
boolean negation;
boolean doubleNegation;

function integer transform() {
	printErr('Postfix operators: ');
	// postfix operators:
	// integer
	intPlus = 10;
	intPlusOrig = intPlus;
	intPlusPlus = intPlus++;
	printErr('integer++ orig: ' + intPlusOrig + ', incremented: ' +  intPlus + ', opresult: ' + intPlusPlus);
	intMinus = 10;
	intMinusOrig = intMinus;
	intMinusMinus = intMinus--;
	printErr('integer-- orig: ' + intMinusOrig + ', decremented: ' +  intMinus + ', opresult: ' + intMinusMinus);
	// long
	longPlus = 10;
	longPlusOrig = longPlus;
	longPlusPlus = longPlus++;
	printErr('long++ orig: ' + longPlusOrig + ', incremented: ' +  longPlus + ', opresult: ' + longPlusPlus);
	longMinus = 10;
	longMinusOrig = longMinus;
	longMinusMinus = longMinus--;
	printErr('long-- orig: ' + longMinusOrig + ', decremented: ' +  longMinus + ', opresult: ' + longMinusMinus);
	//number
	numberPlus = 10.1;
	numberPlusOrig = numberPlus;
	numberPlusPlus = numberPlus++;
	printErr('number++ orig: ' + numberPlusOrig + ', incremented: ' +  numberPlus + ', opresult: ' + numberPlusPlus);
	numberMinus = 10.1;
	numberMinusOrig = numberMinus;
	numberMinusMinus = numberMinus--;
	printErr('number-- orig: ' + numberMinusOrig + ', decremented: ' +  numberMinus + ', opresult: ' + numberMinusMinus);
	// decimal
	decimalPlus = 10.1D;
	decimalPlusOrig = decimalPlus;
	decimalPlusPlus = decimalPlus++;
	printErr('decimal++ orig: ' + decimalPlusOrig + ', incremented: ' +  decimalPlus + ', opresult: ' + decimalPlusPlus);
	decimalMinus = 10.1D;
	decimalMinusOrig = decimalMinus;
	decimalMinusMinus = decimalMinus--;
	printErr('decimal-- orig: ' + decimalMinusOrig + ', decremented: ' +  decimalMinus + ', opresult: ' + decimalMinusMinus);
	
	// prefix operators
	// integer 
	printErr('Prefix operators: ');
	plusInt = 10;
	plusIntOrig = plusInt;
	plusPlusInt = ++plusInt;
	printErr('++integer orig: ' + plusIntOrig + ', incremented: ' +  plusInt + ', opresult: ' + plusPlusInt);
	minusInt = 10;
	unaryInt = -(minusInt);
	minusIntOrig = minusInt;
	minusMinusInt = --minusInt;
	printErr('--integer orig: ' + minusIntOrig + ', decremented: ' +  minusInt + ', opresult: ' + minusMinusInt + ', unary: ' + unaryInt);
	// long
	plusLong = 10;
	plusLongOrig = plusLong;
	plusPlusLong = ++plusLong;
	printErr('++long orig: ' + plusLongOrig + ', incremented: ' +  plusLong + ', opresult: ' + plusPlusLong);
	minusLong = 10;
	unaryLong = -(minusLong);
	minusLongOrig = minusLong;
	minusMinusLong = --minusLong;
	printErr('--long orig: ' + minusLongOrig + ', decremented: ' +  minusLong + ', opresult: ' + minusMinusLong + ', unary: ' + unaryLong);
	// double
	plusNumber = 10.1;
	plusNumberOrig = plusNumber;
	plusPlusNumber = ++plusNumber;
	printErr('++number orig: ' + plusNumberOrig + ', incremented: ' +  plusNumber + ', opresult: ' + plusPlusNumber);
	minusNumber = 10.1;
	unaryNumber = -(minusNumber);
	minusNumberOrig = minusNumber;
	minusMinusNumber = --minusNumber;
	printErr('--number orig: ' + minusNumberOrig + ', decremented: ' +  minusNumber + ', opresult: ' + minusMinusNumber + ', unary: ' + unaryNumber);
	// decimal
	plusDecimal = 10.1D;
	plusDecimalOrig = plusDecimal;
	plusPlusDecimal = ++plusDecimal;
	printErr('++decimal orig: ' + plusDecimalOrig + ', incremented: ' +  plusDecimal + ', opresult: ' + plusPlusDecimal);
	minusDecimal = 10.1D;
	unaryDecimal = -(minusDecimal);
	minusDecimalOrig = minusDecimal;
	minusMinusDecimal = --minusDecimal;
	printErr('--decimal orig: ' + minusDecimalOrig + ', decremented: ' +  minusDecimal + ', opresult: ' + minusMinusDecimal + ', unary: ' + unaryDecimal);
	
	plusPlusRecord.Value = 100;
	++plusPlusRecord.Value;
	minusMinusRecord.Value = 100;
	--minusMinusRecord.Value;
	recordPlusPlus.Value = 100;
	recordPlusPlus.Value++;
	recordMinusMinus.Value = 100;
	recordMinusMinus.Value--;
	
	modifiedPlusPlusRecord.Value = 100;
	plusPlusRecord(modifiedPlusPlusRecord);
	modifiedMinusMinusRecord.Value = 100;
	minusMinusRecord(modifiedMinusMinusRecord);
	modifiedRecordPlusPlus.Value = 100;
	recordPlusPlus(modifiedRecordPlusPlus);
	modifiedRecordMinusMinus.Value = 100;
	recordMinusMinus(modifiedRecordMinusMinus);
	
	// logical negation
	booleanValue = true;
	negation = !booleanValue;
	doubleNegation = !negation;
	printErr('!boolean orig: ' + booleanValue + ' ,not: ' + negation + ' ,double not: ' + doubleNegation);

	return 0;
}

function void recordPlusPlus(firstInput rec) {
	rec.Value++;
}

function void recordMinusMinus(firstInput rec) {
	rec.Value--;
}

function void plusPlusRecord(firstInput rec) {
	++rec.Value;
}

function void minusMinusRecord(firstInput rec) {
	--rec.Value;
}

