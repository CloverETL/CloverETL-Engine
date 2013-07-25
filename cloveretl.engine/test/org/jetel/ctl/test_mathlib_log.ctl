number ln;

number test_int;
number test_long;
number test_double;
number test_decimal;

function integer transform() {
	ln=log(3);
	
	integer int_var = 32;
	long long_var = 14L;
	double double_var = 12.9;
	decimal dec_var = 23.7d;
	
	test_int = log(int_var);
	test_long = log(long_var);
	test_double = log(double_var);
	test_decimal = log(dec_var);
	
	return 0;
}