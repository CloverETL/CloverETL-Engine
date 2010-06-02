integer absIntegerPlus;
integer absIntegerMinus;
long absLongPlus;
long absLongMinus;
double absDoublePlus;
double absDoubleMinus;
decimal absDecimalPlus;
decimal absDecimalMinus;

function integer transform() {
	absIntegerPlus = abs(10);
	absIntegerMinus = abs(-1);
	absLongPlus = abs(10L);
	absLongMinus = abs(-1L);
	absDoublePlus = abs(10.0);
	absDoubleMinus = abs(-1.0);
	number a = 5.0;
	absDecimalPlus = abs(a);
	absDecimalMinus = abs(-a);
		
	return 0;
}