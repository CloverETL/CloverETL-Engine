string[] alphaResult;
string[] bravoResult;
integer[] countResult;
string[] charlieResult;

integer charlieUpdatedCount;
string[] charlieUpdatedResult;
boolean putResult;
 
integer idx;

function integer transform() {
	lookupMetadata tmpRecord;
	tmpRecord.Name = "Charlie";
	tmpRecord.Value = 3;
	tmpRecord.City = "Chodov";
	lookup(TestLookup).put(tmpRecord);

	idx = 0;
	for (integer i=0; i<2; i++) {
		alphaResult[i] = lookup(TestLookup).get('Alpha',1).City;
		bravoResult[i] = lookup(TestLookup).get('Bravo',2).City;
		countResult[i] = lookup(TestLookup).count('Charlie',3);
		for (integer count=0; count<countResult[i]; count++) {
			charlieResult[idx++] = lookup(TestLookup).next().City;
		}
	}
	
	tmpRecord.City = "Cheb";
	putResult = lookup(TestLookup).put(tmpRecord);
	
	tmpRecord.City = "Chrudim";
	lookup(TestLookup).put(tmpRecord);
	
	charlieUpdatedCount = lookup(TestLookup).count('Charlie',3);
	for (integer count = 0; count < charlieUpdatedCount; count++) {
		charlieUpdatedResult[count] = lookup(TestLookup).next().City;
	}
	sort(charlieUpdatedResult);
	
	return 0;
}