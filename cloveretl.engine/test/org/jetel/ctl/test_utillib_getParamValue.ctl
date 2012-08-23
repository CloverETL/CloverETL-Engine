map[string, string] params;
string nonExisting = "NONEXISTING";

function integer transform() {
	foreach(string paramName: getParamValues().getKeys()) {
		params[paramName] = getParamValue(paramName);
	}
	params[nonExisting] = getParamValue(nonExisting);
	
	return 0;
}