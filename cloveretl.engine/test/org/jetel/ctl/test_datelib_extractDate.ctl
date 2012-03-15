date originalDate;
date bornExtractDate;

function integer transform() {
	originalDate = $firstInput.Born;
	bornExtractDate = extractDate(originalDate);
	return 0;
}