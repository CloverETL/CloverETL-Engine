integer[] intRes;
long[] longRes;
string[] stringRes;
integer intCurrent; 
long longCurrent; 
string stringCurrent;

function integer transform() {
	// integer values
	for (integer i=0; i<3; i++) {
		intRes[i] = sequence(TestSequence,integer).next(); 
	}
	print_err(intRes);
	// long values
	sequence(TestSequence).reset();
	for (integer i=0; i<3; i++) {
		longRes[i] = sequence(TestSequence,long).next(); 
	}
	print_err(longRes);
	// string values
	sequence(TestSequence).reset();
	for (integer i=0; i<3; i++) {
		stringRes[i] = sequence(TestSequence,string).next(); 
	}
	print_err(stringRes);
	// current
	intCurrent = sequence(TestSequence,integer).current(); 
	longCurrent = sequence(TestSequence,long).current(); 
	stringCurrent = sequence(TestSequence,string).current();
	print_err('Current integer:' + intCurrent);
	print_err('Current long:' + longCurrent);
	print_err('Current string:' + stringCurrent);
	return 0;
}