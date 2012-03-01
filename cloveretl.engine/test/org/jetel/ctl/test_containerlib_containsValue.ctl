string[] values = ["John", "Johnny", "Little John", "Doe", "Defoe", "Dee", "Jersey", "New York"];
boolean[] results;

function integer transform() {
	for (integer i = 0; i < values.length(); i++) {
		results[i] = $in.multivalueInput.stringMapField.containsValue(values[i]);
	}
	
	return 0;
}