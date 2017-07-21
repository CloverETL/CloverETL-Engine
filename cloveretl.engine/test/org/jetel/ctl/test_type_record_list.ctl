string resultString;
integer resultInt;
string resultString2;
integer resultInt2;

function integer transform() {
	
	firstInput[] recordList;
	firstInput singleRecord;

    singleRecord.Name = "string2";
    singleRecord.Value = 10;

    for (integer i = 0; i < 5; i++) {
    	recordList[i].Name = "string";
    	recordList[i].Value = 2*i;
    }
    recordList[11] = singleRecord;
	
	resultString = recordList[3].Name;
	resultInt = recordList[3].Value;
	resultString2 = recordList[11].Name;
	resultInt2 = recordList[11].Value;

	return 0;
}
