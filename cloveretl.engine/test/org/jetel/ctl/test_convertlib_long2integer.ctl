integer fromLong1;
integer fromLong2;
integer nullRet1;
integer nullRet2;

function integer transform() {
	fromLong1 = long2integer(10l);
	fromLong2 = long2integer(-10);
	long l = null;
	nullRet1 = long2integer(l);
	nullRet2 = long2integer(null);
	return 0;
}