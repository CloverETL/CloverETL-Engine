number sqrtPi;
number sqrt9;

number test_int;
number test_long;
number test_num;
number test_dec;

function integer transform() {
	integer i=9;

	sqrtPi=sqrt(pi());
	sqrt9=sqrt(i);
	
	integer var_int = 4;
	test_int = sqrt(var_int);
	
	long var_long = 64L;
	test_long = sqrt(var_long);
	
	number var_num = 86.9;
	test_num = sqrt(var_num);
	
	decimal var_dec = 34.5d;
	test_dec = sqrt(var_dec);
	
	return 0;
}