map[string, string] params;

function integer transform() {
	foreach(string paramName: getParamValues().getKeys()) {
		params[paramName] = getParamValue(paramName);
	}
	
	return 0;
}