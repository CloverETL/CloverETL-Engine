firstOutput[] firstRecordList;
firstOutput[] secondRecordList;

multivalueOutput record1;
	
function integer transform() {
	firstOutput myRecord;
	
	firstRecordList[0] = myRecord;
	
	firstRecordList[0].Name = "before";
	
	secondRecordList = firstRecordList; // makes a deep copy

	firstRecordList[0].Name = "after";
	
	//--------------------------------
	
	record1 = $in.3;
	record1.stringListField[0] = "first";
	$out.4 = record1;
	record1.stringListField[1] = "second";
	
	// record1 == ["first", "second"]
	// $in.3 == []
	// $out.4 == ["first"]
	
	return 0;
}