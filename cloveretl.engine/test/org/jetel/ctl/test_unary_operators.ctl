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
	print_err('Postfix operators: ');
	// postfix operators:
	// integer
	intPlus = 10;
	intPlusOrig = intPlus;
	intPlusPlus = intPlus++;
	print_err('integer++ orig: ' + intPlusOrig + ', incremented: ' +  intPlus + ', opresult: ' + intPlusPlus);
	intMinus = 10;
	intMinusOrig = intMinus;
	intMinusMinus = intMinus--;
	print_err('integer-- orig: ' + intMinusOrig + ', decremented: ' +  intMinus + ', opresult: ' + intMinusMinus);
	// long
	longPlus = 10;
	longPlusOrig = longPlus;
	longPlusPlus = longPlus++;
	print_err('long++ orig: ' + longPlusOrig + ', incremented: ' +  longPlus + ', opresult: ' + longPlusPlus);
	longMinus = 10;
	longMinusOrig = longMinus;
	longMinusMinus = longMinus--;
	print_err('long-- orig: ' + longMinusOrig + ', decremented: ' +  longMinus + ', opresult: ' + longMinusMinus);
	//number
	numberPlus = 10.1;
	numberPlusOrig = numberPlus;
	numberPlusPlus = numberPlus++;
	print_err('number++ orig: ' + numberPlusOrig + ', incremented: ' +  numberPlus + ', opresult: ' + numberPlusPlus);
	numberMinus = 10.1;
	numberMinusOrig = numberMinus;
	numberMinusMinus = numberMinus--;
	print_err('number-- orig: ' + numberMinusOrig + ', decremented: ' +  numberMinus + ', opresult: ' + numberMinusMinus);
	// decimal
	decimalPlus = 10.1D;
	decimalPlusOrig = decimalPlus;
	decimalPlusPlus = decimalPlus++;
	print_err('decimal++ orig: ' + decimalPlusOrig + ', incremented: ' +  decimalPlus + ', opresult: ' + decimalPlusPlus);
	decimalMinus = 10.1D;
	decimalMinusOrig = decimalMinus;
	decimalMinusMinus = decimalMinus--;
	print_err('decimal-- orig: ' + decimalMinusOrig + ', decremented: ' +  decimalMinus + ', opresult: ' + decimalMinusMinus);
	
	// prefix operators
	// integer 
	print_err('Prefix operators: ');
	plusInt = 10;
	plusIntOrig = plusInt;
	plusPlusInt = ++plusInt;
	print_err('++integer orig: ' + plusIntOrig + ', incremented: ' +  plusInt + ', opresult: ' + plusPlusInt);
	minusInt = 10;
	unaryInt = -(minusInt);
	minusIntOrig = minusInt;
	minusMinusInt = --minusInt;
	print_err('--integer orig: ' + minusIntOrig + ', decremented: ' +  minusInt + ', opresult: ' + minusMinusInt + ', unary: ' + unaryInt);
	// long
	plusLong = 10;
	plusLongOrig = plusLong;
	plusPlusLong = ++plusLong;
	print_err('++long orig: ' + plusLongOrig + ', incremented: ' +  plusLong + ', opresult: ' + plusPlusLong);
	minusLong = 10;
	unaryLong = -(minusLong);
	minusLongOrig = minusLong;
	minusMinusLong = --minusLong;
	print_err('--long orig: ' + minusLongOrig + ', decremented: ' +  minusLong + ', opresult: ' + minusMinusLong + ', unary: ' + unaryLong);
	// double
	plusNumber = 10.1;
	plusNumberOrig = plusNumber;
	plusPlusNumber = ++plusNumber;
	print_err('++number orig: ' + plusNumberOrig + ', incremented: ' +  plusNumber + ', opresult: ' + plusPlusNumber);
	minusNumber = 10.1;
	unaryNumber = -(minusNumber);
	minusNumberOrig = minusNumber;
	minusMinusNumber = --minusNumber;
	print_err('--number orig: ' + minusNumberOrig + ', decremented: ' +  minusNumber + ', opresult: ' + minusMinusNumber + ', unary: ' + unaryNumber);
	// decimal
	plusDecimal = 10.1D;
	plusDecimalOrig = plusDecimal;
	plusPlusDecimal = ++plusDecimal;
	print_err('++decimal orig: ' + plusDecimalOrig + ', incremented: ' +  plusDecimal + ', opresult: ' + plusPlusDecimal);
	minusDecimal = 10.1D;
	unaryDecimal = -(minusDecimal);
	minusDecimalOrig = minusDecimal;
	minusMinusDecimal = --minusDecimal;
	print_err('--decimal orig: ' + minusDecimalOrig + ', decremented: ' +  minusDecimal + ', opresult: ' + minusMinusDecimal + ', unary: ' + unaryDecimal);
	
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
	print_err('!boolean orig: ' + booleanValue + ' ,not: ' + negation + ' ,double not: ' + doubleNegation);

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

