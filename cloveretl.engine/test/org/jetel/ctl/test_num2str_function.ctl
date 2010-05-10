string[] intOutput;
string[] longOutput;
string[] doubleOutput;
string[] decimalOutput;
		
function integer transform() {
	// integer conversions
	intOutput[0] = num2str(16);
	intOutput[1] = num2str(16,2);
	intOutput[2] = num2str(16,8);
	intOutput[3] = num2str(16,16);
	print_err("integer output: " + intOutput);

	// long conversions
	longOutput[0] = num2str(16);
	longOutput[1] = num2str(16,2);
	longOutput[2] = num2str(16,8);
	longOutput[3] = num2str(16,16);
	print_err("long output: " + longOutput);

	// double conversions
	doubleOutput[0] = num2str(16.16);
	doubleOutput[1] = num2str(16.16,16);
	print_err("double output: " + doubleOutput);

	// decimal conversions
	decimalOutput[0] = num2str(16.16D);
	print_err("decimal output: " + decimalOutput);
	
	return 0;
}