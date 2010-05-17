boolean trueValue;
boolean falseValue;
boolean res1;
boolean res2;
boolean res3;
boolean res4;
decimal d;
boolean res5; 
boolean res6;
decimal e;
boolean res7; 
boolean res8;

function integer transform() {
	trueValue = true;
	falseValue = false;
	// and
	res1 = trueValue && falseValue;
	res2 = falseValue && trueValue;
	// or
	res3 = trueValue || falseValue;
	res4 = falseValue || trueValue;
	// lazy evaluation AND: result is if-statement
	d = 10;
	res5 = d++ < 10 && trueValue; 
	res6 = d++ <10 && d++ < 11;
	// lazy evaluation OR: result is if-statement
	e = 10;
	res7 = e++ < 10 || trueValue; 
	res8 = e++ <10 || e++ < 11;
	return 0;
}