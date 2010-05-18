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
	print_err(ret);
	// integer fields
	print_err('foreach1: integer fields'); 
	i=0; 
	foreach (integer intVal : $firstInput.*) {
		intRes[i++]=intVal;
		print_err('integer: ' + intRes);
	}
	// long fields
	print_err('foreach2: long fields'); 
	i=0; 
	foreach (long longVal : $firstInput.*) {
		longRes[i++]=longVal;
		print_err('long: ' + longRes);
	}
	// double fields
	print_err('foreach3: double fields'); 
	i=0; 
	foreach (double doubleVal : $firstInput.*) {
		doubleRes[i++]=doubleVal;
		print_err('double: ' + doubleRes);
	}
	// decimal fields
	print_err('foreach5: decimal fields'); 
	i=0; 
	foreach (decimal decimalVal : $firstInput.*) {
		decimalRes[i++]=decimalVal;
		print_err('decimal: ' + decimalRes);
	}
	// boolean fields
	print_err('foreach4: boolean fields'); 
	i=0; 
	foreach (boolean booleanVal : $firstInput.*) {
		booleanRes[i++]=booleanVal;
		print_err('boolean: ' + booleanRes);
	}
	// string fields
	print_err('foreach6: string fields'); 
	i=0; 
	foreach (string stringVal : $firstInput.*) {
		stringRes[i++]=stringVal;
		print_err('string: ' + stringRes);
	}
	// date fields
	print_err('foreach7: date fields'); 
	i=0; 
	foreach (date dateVal : $firstInput.*) {
		dateRes[i++]=dateVal;
		print_err('date: ' + dateRes);
	}
	tmpVar.Value = 123;
	foreach (integer i: tmpVar.*) {
		print_err('integer : ' + i);
	}
	return 0;
}