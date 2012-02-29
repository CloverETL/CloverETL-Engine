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

map[integer, string] integerStringMap;
map[string, integer] stringIntegerMap;
map[string, firstInput] stringRecordMap;
string[] integerStringMapRes;
integer[] stringIntegerMapRes;
firstInput[] stringRecordMapRes;

function integer transform() {
	// iterating over list
	it = [ 'a', 'b', 'c' ];
	string ret = '';
	foreach (string s : it) {
		ret = ret + s;
	}
	printErr(ret);
	
	// iterating over a map
	for (integer n = 0; n < 5; n++) {
		integerStringMap[n] = num2str(n);
		stringIntegerMap[num2str(n)] = n;
		firstInput tmpRecord;
		tmpRecord.Name = "A string";
		tmpRecord.Value = n;
		stringRecordMap[num2str(n)] = tmpRecord;
	} 
	
	foreach (string strVal : integerStringMap) {
		integerStringMapRes[integerStringMapRes.length()] = strVal;
	}
	foreach (integer intVal : stringIntegerMap) {
		stringIntegerMapRes[stringIntegerMapRes.length()] =  intVal;
	}
	foreach (firstInput recVal : stringRecordMap) {
		stringRecordMapRes[stringRecordMapRes.length()] = recVal;
	}
	
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