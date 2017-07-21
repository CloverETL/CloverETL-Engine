long bornDate;
long zeroDate;
long nullRet1;
long nullRet2;

function integer transform() {
	bornDate = date2long($firstInput.Born);
	zeroDate = date2long(zeroDate());
	date d = null;
	nullRet1 = date2long(d);
	nullRet2 = date2long(null);
	return 0;
}