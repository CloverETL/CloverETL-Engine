string[] fieldNames;

function integer transform() {
	for (integer i = 0; i < length($0.*); i++) {
		fieldNames[i] = getFieldName($0.*, i);	
	}
	return 0;
}