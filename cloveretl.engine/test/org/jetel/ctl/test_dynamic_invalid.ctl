// Transforms input record into output record.
function integer transform() {

	$in.0.Name = "hello";
	$in.0.Value = 25;
	
	setStringValue($0.*, "fieldName", "value");
	
	return ALL;
}
