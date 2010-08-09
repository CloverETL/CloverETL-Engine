function integer transform() {
	// assign values from first input record to fourth output record
	// fourth output record is sub-record of first one
	fourthOutput outputRec;
	copyByName(outputRec.*, $0.*);
	$3.* = outputRec.*;
	
	return 0;
}