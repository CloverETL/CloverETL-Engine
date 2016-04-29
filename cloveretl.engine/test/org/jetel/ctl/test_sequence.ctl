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
	for (integer i=3; i<6; i++) {
		$out.0.Value = sequence(TestSequence,integer).next();
		intRes[i] = $out.0.Value;
	}
	printErr("int:    " + intRes);
		
	// long values
	sequence(TestSequence).reset();
	for (integer i=0; i<3; i++) {
		longRes[i] = sequence(TestSequence,long).next(); 
	}
	for (integer i=3; i<6; i++) {
		$out.0.BornMillisec = sequence(TestSequence,long).next();
		longRes[i] = $out.0.BornMillisec;  
	}
	printErr("long:   " + longRes);
	
	// string values
	sequence(TestSequence).reset();
	for (integer i=0; i<3; i++) {
		stringRes[i] = sequence(TestSequence,string).next(); 
	}
	for (integer i=3; i<6; i++) {
		$out.0.Name = sequence(TestSequence,string).next();
		stringRes[i] = $out.0.Name; 
	}
	printErr("string: " + stringRes);
	
	// current
	intCurrent = sequence(TestSequence,integer).current(); 
	longCurrent = sequence(TestSequence,long).current(); 
	stringCurrent = sequence(TestSequence,string).current();
	printErr('Current integer: ' + intCurrent);
	printErr('Current long:    ' + longCurrent);
	printErr('Current string:  ' + stringCurrent);
	return 0;
}