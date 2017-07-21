date originalDate;
date bornExtractTime;

function integer transform() {
	originalDate = $firstInput.Born;
	bornExtractTime = extractTime(originalDate);
	return 0;
}