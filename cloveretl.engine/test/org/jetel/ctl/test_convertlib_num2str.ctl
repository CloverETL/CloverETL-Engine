string[] intOutput;
string[] longOutput;
string[] doubleOutput;
string[] decimalOutput;
string test_null_dec;
function integer transform() {

	// integer conversions
	intOutput[0] = num2str(16);
	intOutput[1] = num2str(16,2);
	intOutput[2] = num2str(16,8);
	intOutput[3] = num2str(16,16);
	intOutput[4] = num2str(1235,'0.###E0');
	intOutput[5] = num2str(12350001,'###,###.# Kcs', 'cs.CZ');
	printErr("integer output: " + intOutput);

	// long conversions
	longOutput[0] = num2str(16);
	longOutput[1] = num2str(16,2);
	longOutput[2] = num2str(16,8);
	longOutput[3] = num2str(16,16);
	longOutput[4] = num2str(12352387956654L,'0.###E0');
	longOutput[5] = num2str(12350001L,'###,###.# Kcs', 'cs.CZ');
	printErr("long output: " + longOutput);

	// double conversions
	doubleOutput[0] = num2str(16.16);
	doubleOutput[1] = num2str(16.16,16);
	doubleOutput[2] = num2str(1235.48,'###.###E0');
	doubleOutput[3] = num2str(12350001.1,'###,###.# Kcs', 'cs.CZ');
	printErr("double output: " + doubleOutput);

	// decimal conversions
	decimalOutput[0] = num2str(16.16D);
	decimalOutput[1] = num2str(1235.44D, '###.###');
	decimalOutput[2] = num2str(12350001.1d, '###,###.# Kcs', 'cs.CZ');
	printErr("decimal output: " + decimalOutput);
	
	decimal d = null;
	test_null_dec = num2str(d);
	return 0;
}