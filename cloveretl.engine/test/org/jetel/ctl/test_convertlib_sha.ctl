byte shaHash1;
byte shaHash2;
byte test_empty;

function integer transform() {
	shaHash1 = sha('The quick brown fox jumps over the lazy dog');
	shaHash2 = sha($firstInput.ByteArray);
	test_empty = sha('');
	return 0;
}