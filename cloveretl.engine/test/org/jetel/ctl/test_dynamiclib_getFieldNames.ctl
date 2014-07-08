string[] fieldNames1;
string[] fieldNames2;
string[] fieldNames3;
string[] fieldNames4;

function integer transform() {
	firstInput myRecord;
	fieldNames1 = getFieldNames($out.3);
	fieldNames2 = getFieldNames(myRecord);

	// must not modify the underlying metadata
	append(fieldNames2, "additionalString");
	fieldNames3 = getFieldNames(myRecord).append("additionalString2"); // should this be allowed?
	
	fieldNames4 = getFieldNames(myRecord);
	return 0;
}