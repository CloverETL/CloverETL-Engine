string ret1;
string ret2;
string ret3;
string ret4;
string ret5;
string ret6; 

function integer transform() {
	ret1 = getFieldPropertyValue($in.0, 1, "myProperty");
	ret2 = getFieldPropertyValue($in.0, "Age", "myProperty");
	ret3 = getFieldPropertyValue($in.0, "Name", "myProperty"); // other field
	ret4 = getFieldPropertyValue($in.0, "Age", "nonExistingProperty");
	ret5 = getFieldPropertyValue($in.1, "Age", "myProperty"); // other record
	
	firstInput myRecord;
	ret6 = getFieldPropertyValue(myRecord, "Age", "myProperty");
	
	return 0;
}