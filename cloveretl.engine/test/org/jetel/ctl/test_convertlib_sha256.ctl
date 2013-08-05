byte shaHash1;
byte shaHash2;
byte test_empty;

function integer transform() {
	shaHash1 = sha256('The quick brown fox jumps over the lazy dog');
	shaHash2 = sha256($firstInput.ByteArray);
	test_empty = sha256('');
	return 0;
}