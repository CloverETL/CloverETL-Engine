integer toInteger;
integer toInteger2;
integer toInteger3;

function integer transform() {
	decimal decimal1 = 0.007d;
	decimal decimal2 = -500.123d;
	decimal decimal3 = 1000000.0d;
	toInteger = decimal2integer(decimal1);
	toInteger2 = decimal2integer(decimal2);
	toInteger3 = decimal2integer(decimal3);
	return 0;
}