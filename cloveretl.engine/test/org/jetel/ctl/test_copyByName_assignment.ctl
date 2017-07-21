function integer transform() {
	// assign values from first input record to fourth output record
	// fourth output record is sub-record of first one
	$3.* = $0.*;
	
	return 0;
}