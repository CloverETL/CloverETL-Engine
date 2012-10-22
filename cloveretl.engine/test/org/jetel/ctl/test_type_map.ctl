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

	map[string, string] fieldAsKey;
	fieldAsKey[$in.0.Name] = $in.0.Name;

	$out.firstMultivalueOutput.stringMapField = fieldAsKey;
	$out.firstMultivalueOutput.stringMapField[$in.0.Name] = $in.0.Name;

	return 0;
}