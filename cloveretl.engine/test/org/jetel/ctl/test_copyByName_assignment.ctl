firstInput inRecord;
firstInput outRecord1;
firstInput outRecord2;

function void copy(firstInput r1, firstInput r2) {
	copyByName(r1, r2); // same TLFunctionCallContext
}

function integer transform() {
	// assign values from first input record to fourth output record
	// fourth output record is sub-record of first one
	$3.* = $0.*;
	
	// CLO-637:
	copy(outRecord1, inRecord); // outRecord1.Name == null
	inRecord.Name = "some value";
	copy(outRecord2, inRecord); // outRecord2.Name == "some value"
	
	return 0;
}