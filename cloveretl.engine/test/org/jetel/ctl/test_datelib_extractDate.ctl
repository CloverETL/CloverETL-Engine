date originalDate;
date bornExtractDate;
date nullDate;
date nullDate2;

function integer transform() {
	originalDate = $firstInput.Born;
	bornExtractDate = extractDate(originalDate);
	nullDate = extractDate(null);
	date d = null;
	nullDate2 = extractDate(d);
	return 0;
}