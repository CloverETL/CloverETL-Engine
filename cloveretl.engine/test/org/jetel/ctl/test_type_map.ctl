map[string,integer] testMap;
map[date,string] dayInWeek;
map[date,string] tuesday;
map[date,string] dayInWeekCopy;
map[date,string] wednesday;

function integer transform() {
	testMap['zero'] = 1; 
	testMap['one'] = 2;
	testMap['two'] = testMap['zero'] + testMap['one'];
	testMap['three'] = 3; 
	testMap['three'] = 4;

	// map concatenation
	dayInWeek[2009-03-02] = 'Monday';
	dayInWeek[2009-03-03] = 'unknown';

	tuesday[2009-03-03] = 'Tuesday';
	dayInWeekCopy = dayInWeek + tuesday;

	wednesday[2009-03-04] = 'Wednesday';
	dayInWeekCopy = dayInWeekCopy + wednesday;

	return 0;
}