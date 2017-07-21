date fromLong1;
date fromLong2;
date fromLong3;
date nullRet1;
date nullRet2;


function integer transform() {
	fromLong1 = long2date(0L);
	fromLong2 = long2date(50000000000L);
	fromLong3 = long2date(-5000L);
	long l = null;
	nullRet1 = long2date(l);
	nullRet2 = long2date(null);
	return 0;
}