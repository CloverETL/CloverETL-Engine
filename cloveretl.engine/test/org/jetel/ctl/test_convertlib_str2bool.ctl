boolean fromTrueString;
boolean fromFalseString;
boolean nullRet1;
boolean nullRet2;

function integer transform() {
	fromTrueString = str2bool("true");
	fromFalseString = str2bool("false");
	string boo = null;
	nullRet1 = str2bool(boo);
	nullRet2 = str2bool(null);
	return 0;
}