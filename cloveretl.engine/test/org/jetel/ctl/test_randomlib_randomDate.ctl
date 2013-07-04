date noTimeZone1;
date noTimeZone2;
date withTimeZone1;
date withTimeZone2;

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
	
	return 0;
}