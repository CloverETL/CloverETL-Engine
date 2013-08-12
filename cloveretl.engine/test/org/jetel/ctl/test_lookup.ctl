string[] alphaResult;
string[] bravoResult;
integer[] countResult;
string[] charlieResult;

integer charlieUpdatedCount;
string[] charlieUpdatedResult;
boolean putResult;
 
integer idx;

lookupMetadata meta;
lookupMetadata meta2;
lookupMetadata meta3;
lookupMetadata meta4;
string strRet;
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
	
	meta = lookup(TestLookup).get('Bravo',1);
	meta2 = lookup(TestLookup).get('Bravo',1).*;
	meta3 = lookup(TestLookup).get(null,1);
	meta4 = lookup(TestLookup).get(null,null);
	lookupMetadata meta5;
	meta5.City = 'Bratislava';
	lookup(TestLookup).put(meta5);
	lookupMetadata meta6 = lookup(TestLookup).get(null,null);
	strRet = meta6.City;
	return 0;
}