string[] intOutput;
string[] longOutput;
string[] doubleOutput;
string[] decimalOutput;
		
function integer transform() {
	// TODO: please test formats thoroughly, this one only tests format cache initialization
	num2str(1235,'0.###E0');

	// integer conversions
	intOutput[0] = num2str(16);
	intOutput[1] = num2str(16,2);
	intOutput[2] = num2str(16,8);
	intOutput[3] = num2str(16,16);
	printErr("integer output: " + intOutput);

	// long conversions
	longOutput[0] = num2str(16);
	longOutput[1] = num2str(16,2);
	longOutput[2] = num2str(16,8);
	longOutput[3] = num2str(16,16);
	printErr("long output: " + longOutput);

	// double conversions
	doubleOutput[0] = num2str(16.16);
	doubleOutput[1] = num2str(16.16,16);
	printErr("double output: " + doubleOutput);

	// decimal conversions
	decimalOutput[0] = num2str(16.16D);
	printErr("decimal output: " + decimalOutput);
	
	return 0;
}