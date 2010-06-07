byte md5Hash1;
byte md5Hash2;

function integer transform() {
	md5Hash1 = md5('The quick brown fox jumps over the lazy dog');
	md5Hash2 = md5($firstInput.ByteArray);
	return 0;
}