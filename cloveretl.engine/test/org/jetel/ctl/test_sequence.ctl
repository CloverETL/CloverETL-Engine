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
	printErr(intRes);
	// long values
	sequence(TestSequence).reset();
	for (integer i=0; i<3; i++) {
		longRes[i] = sequence(TestSequence,long).next(); 
	}
	printErr(longRes);
	// string values
	sequence(TestSequence).reset();
	for (integer i=0; i<3; i++) {
		stringRes[i] = sequence(TestSequence,string).next(); 
	}
	printErr(stringRes);
	// current
	intCurrent = sequence(TestSequence,integer).current(); 
	longCurrent = sequence(TestSequence,long).current(); 
	stringCurrent = sequence(TestSequence,string).current();
	printErr('Current integer:' + intCurrent);
	printErr('Current long:' + longCurrent);
	printErr('Current string:' + stringCurrent);
	return 0;
}