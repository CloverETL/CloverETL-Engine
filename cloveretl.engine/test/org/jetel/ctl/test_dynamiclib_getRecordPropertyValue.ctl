string ret1;
string ret2;
string ret3;
string ret4;
string ret5;

function integer transform() {
	ret1 = getRecordPropertyValue($in.0, "myProperty");
	ret2 = getRecordPropertyValue($out.0, "myProperty");
	ret3 = getRecordPropertyValue($in.3, "myProperty");
	
	firstInput myRecord;
	ret4 = getRecordPropertyValue(myRecord, "myProperty");
	
	ret5 = getRecordPropertyValue($in.0, "nonExistingProperty");
	
	return 0;
}