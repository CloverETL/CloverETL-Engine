boolean integerTrue;
boolean integerFalse;
boolean longTrue;
boolean longFalse;
boolean doubleTrue;
boolean doubleFalse;
boolean decimalTrue;
boolean decimalFalse;
boolean nullInt;
boolean nullLong;
boolean nullDouble;
boolean nullDecimal;


function integer transform() {
	integerTrue = num2bool(-30);
	integerFalse = num2bool(0);
	longTrue = num2bool(300L);
	longFalse = num2bool(0L);
	doubleTrue = num2bool(-30.654);
	doubleFalse = num2bool(0.0);
	decimal inputTrue = 122.654;
	decimal inputFalse = 0.0;
	decimalTrue = num2bool(inputTrue);
	decimalFalse = num2bool(inputFalse);
	integer int = null;
	long l = null;
	number num = null;
	decimal dec = null;
	nullInt = num2bool(int);
	nullLong = num2bool(l);
	nullDouble = num2bool(num);
	nullDecimal = num2bool(dec);
	return 0;
}