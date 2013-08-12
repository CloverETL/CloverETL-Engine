date originalDate;
date bornExtractTime;
date nullDate;
date nullDate2;

function integer transform() {
	originalDate = $firstInput.Born;
	bornExtractTime = extractTime(originalDate);
	date d = null;
	nullDate = extractTime(d);
	nullDate2 = extractTime(null);
	return 0;
}