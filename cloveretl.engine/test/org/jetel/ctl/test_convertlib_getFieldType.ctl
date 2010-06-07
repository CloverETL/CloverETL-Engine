string[] fieldTypes;

function integer transform() {
	for (integer i = 0; i < length($0); i++) {
		fieldTypes[i] = getFieldType($0.*, i);	
	}
	return 0;
}