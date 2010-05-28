string[] it;
string ret;
integer[] intRes;
integer i; 
long[] longRes;
double[] doubleRes;
decimal[] decimalRes;
boolean[] booleanRes;
string[] stringRes;
date[] dateRes;
firstInput tmpVar; 

function integer transform() {
	// iterating over list
	it = [ 'a', 'b', 'c' ];
	string ret = '';
	foreach (string s : it) {
		ret = ret + s;
	}
	printErr(ret);
	// integer fields
	printErr('foreach1: integer fields'); 
	i=0; 
	foreach (integer intVal : $firstInput.*) {
		intRes[i++]=intVal;
		printErr('integer: ' + intRes);
	}
	// long fields
	printErr('foreach2: long fields'); 
	i=0; 
	foreach (long longVal : $firstInput.*) {
		longRes[i++]=longVal;
		printErr('long: ' + longRes);
	}
	// double fields
	printErr('foreach3: double fields'); 
	i=0; 
	foreach (double doubleVal : $firstInput.*) {
		doubleRes[i++]=doubleVal;
		printErr('double: ' + doubleRes);
	}
	// decimal fields
	printErr('foreach5: decimal fields'); 
	i=0; 
	foreach (decimal decimalVal : $firstInput.*) {
		decimalRes[i++]=decimalVal;
		printErr('decimal: ' + decimalRes);
	}
	// boolean fields
	printErr('foreach4: boolean fields'); 
	i=0; 
	foreach (boolean booleanVal : $firstInput.*) {
		booleanRes[i++]=booleanVal;
		printErr('boolean: ' + booleanRes);
	}
	// string fields
	printErr('foreach6: string fields'); 
	i=0; 
	foreach (string stringVal : $firstInput.*) {
		stringRes[i++]=stringVal;
		printErr('string: ' + stringRes);
	}
	// date fields
	printErr('foreach7: date fields'); 
	i=0; 
	foreach (date dateVal : $firstInput.*) {
		dateRes[i++]=dateVal;
		printErr('date: ' + dateRes);
	}
	tmpVar.Value = 123;
	foreach (integer i: tmpVar.*) {
		printErr('integer : ' + i);
	}
	return 0;
}