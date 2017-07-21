byte shaHash1;
byte shaHash2;

function integer transform() {
	shaHash1 = sha('The quick brown fox jumps over the lazy dog');
	shaHash2 = sha($firstInput.ByteArray);
	return 0;
}