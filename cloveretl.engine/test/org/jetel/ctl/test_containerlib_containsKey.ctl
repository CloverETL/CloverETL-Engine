string[] keys = ["name", "firstName", "given name", "lastName", "fullName", "address"];
boolean[] results;

function integer transform() {
	for (integer i = 0; i < keys.length(); i++) {
		results[i] = $in.multivalueInput.stringMapField.containsKey(keys[i]);
	}
	
	return 0;
}