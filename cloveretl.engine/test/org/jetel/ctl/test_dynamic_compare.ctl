integer[] compare;
integer[] compareBooleans;

// Transforms input record into output record.
function integer transform() {

	// compare integers
	for(integer i = 0; i < 9; i++) {
		$out.0.Value = i;
		$out.1.Value = i + 5 * ((i % 3) - 1);
		compare[i] = compare($out.0, "Value", $out.1, "Value"); // field names 
	}
	
	// compare strings
	for(integer i = 9; i < 18; i++) {
		string randomSuffix = randomString(3, 5);
		$out.0.Name = "b" + randomSuffix;
		$out.1.Name = charAt("abc", i % 3) + randomSuffix;
		compare[i] = compare($out.0, 0, $out.1, 0); // field indices
	}
	
	// compare dates
	for(integer i = 18; i < 27; i++) {
		$out.0.Born = long2date(i);
		$out.1.Born = long2date(i + 5 * ((i % 3) - 1));
		compare[i] = compare($out.0, "Born", $out.1, "Born"); 
	}
	
	// compare numeric fields
	for(integer i = 27; i < 36; i++) {
		$out.0.Age = i + 0.125;
		$out.1.Age = i + 5 * ((i % 3) - 1) + 0.125;
		compare[i] = compare($out.0, "Age", $out.1, "Age"); 
	}
	
	// compare decimals
	for(integer i = 36; i < 45; i++) {
		$out.0.Currency = i + 0.125;
		$out.1.Currency = i + 5 * ((i % 3) - 1) + 0.125;
		compare[i] = compare($out.0, "Currency", $out.1, "Currency"); 
	}

	// compare booleans
	integer cb = 0;	
	$out.0.Flag = true;
	$out.1.Flag = true;
	compareBooleans[cb++] = compare($out.0, "Flag", $out.1, "Flag");
	$out.0.Flag = true;
	$out.1.Flag = false;
	compareBooleans[cb++] = compare($out.0, "Flag", $out.1, "Flag");
	$out.0.Flag = false;
	$out.1.Flag = true;
	compareBooleans[cb++] = compare($out.0, "Flag", $out.1, "Flag");
	$out.0.Flag = false;
	$out.1.Flag = false;
	compareBooleans[cb++] = compare($out.0, "Flag", $out.1, "Flag");
	
	return ALL;
}
