map[string, string] params;
string nonExisting = "NONEXISTING";

function integer transform() {
	params[nonExisting] = getParamValue(nonExisting);
	
	return 0;
}