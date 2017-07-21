boolean[] results;

function integer transform() {
	results[0] = $in.multivalueInput.stringListField.containsAll(["John", "Doe", "John"]);
	results[1] = $in.multivalueInput.stringListField.containsAll(["John", "Doe", null]);
	results[2] = $in.multivalueInput.stringListField.containsAll(["John", "Doe", "Jersey"]);
	results[3] = $in.multivalueInput.stringListField.containsAll(["John", "Dee"]);
	
	integer[] emptyList;
	results[4] = $in.multivalueInput.integerListField.containsAll(emptyList);
	results[5] = $in.multivalueInput.integerListField.containsAll([123]);
	results[6] = $in.multivalueInput.integerListField.containsAll([789, 456]);
	results[7] = $in.multivalueInput.integerListField.containsAll([1234, 123]);
	results[8] = $in.multivalueInput.integerListField.containsAll([789, 123, 456]);
	results[9] = $in.multivalueInput.integerListField.containsAll([789, 123, 456, 123]);
	results[10] = $in.multivalueInput.integerListField.containsAll([789, 123, 4567, 123]);
	
	return 0;
}