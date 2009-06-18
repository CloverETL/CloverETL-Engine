function int transform() {
	// integer conversions
	string[] intOutput;
	intOutput[0] = num2str(16);
	intOutput[1] = num2str(16,2);
	intOutput[2] = num2str(16,8);
	intOutput[3] = num2str(16,16);
	print_err("int output: " + intOutput);

	// long conversions
	string[] longOutput;
	longOutput[0] = num2str(16);
	longOutput[1] = num2str(16,2);
	longOutput[2] = num2str(16,8);
	longOutput[3] = num2str(16,16);
	print_err("long output: " + longOutput);

	// double conversions
	string[] doubleOutput;
	doubleOutput[0] = num2str(16.16);
	doubleOutput[1] = num2str(16.16,16);
	print_err("double output: " + doubleOutput);

	// decimal conversions
	string[] decimalOutput;
	decimalOutput[0] = num2str(16.16D);
	print_err("decimal output: " + decimalOutput);
	
	return 0;
}