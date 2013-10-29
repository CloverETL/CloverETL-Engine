date noTimeZone1;
date noTimeZone2;
date withTimeZone1;
date withTimeZone2;
date patt_null;
date ret1;
date ret2;
date ret3;
date ret4;

function integer transform() {
	setRandomSeed(0);
	
	string pattern = "dd.MM.yyyy HH:mm:ss";
	
	string noTimeZoneStr = date2str($firstInput.Born, pattern);
	string withTimeZone1Str = date2str($firstInput.Born, pattern, 'en', 'GMT+5');
	string withTimeZone2Str = date2str($firstInput.Born, pattern, 'en', 'GMT-5');
	
	noTimeZone1 = randomDate($firstInput.Born, $firstInput.Born);
	noTimeZone2 = randomDate(noTimeZoneStr, noTimeZoneStr, pattern);
	withTimeZone1 = randomDate(withTimeZone1Str, withTimeZone1Str, pattern, 'en', 'GMT+3');
	withTimeZone2 = randomDate(withTimeZone2Str, withTimeZone2Str, pattern, 'en', 'GMT-3');
	pattern = null; //engine uses default pattern 'yyyy-MM-dd'
	patt_null = randomDate('2006-10-12','2010-11-12',pattern);
	ret1 = randomDate('2006-10-12','2010-11-12', null, null);
	ret2 = randomDate('2006-10-12','2010-11-12', null, null, null);
	ret3 = randomDate(1997-11-12, 1997-11-12);
	ret4 = randomDate(12000, 12000);
	return 0;
}