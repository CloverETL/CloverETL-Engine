number varLog10;

number test_int;
number test_long;
number test_decimal;
number test_number;

function integer transform() {
	varLog10=log10(3);
	
	integer var_int = 5;
	test_int = log10(var_int);
	long var_long = 90L;
	test_long = log10(var_long);
	decimal var_dec = 32.1d;
	test_decimal = log10(var_dec);
	number var_num = 84.12;
	test_number = log10(var_num);
	return 0;
}