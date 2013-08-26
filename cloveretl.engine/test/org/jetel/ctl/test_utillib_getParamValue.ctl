map[string, string] params;
string nonExisting = "NONEXISTING";
string ret1;
string ret2;

function integer transform() {
	foreach(string paramName: getParamValues().getKeys()) {
		params[paramName] = getParamValue(paramName);
	}
	params[nonExisting] = getParamValue(nonExisting);
	string str = null;
	ret1 = getParamValue(null);
	ret2 = getParamValue(str);
	printErr(ret2);
	return 0;
}