integer resultTrue;
integer resultFalse;
integer nullRet1;
integer nullRet2;

function integer transform() {
	resultTrue = bool2num(true);
	resultFalse = bool2num(false);
	boolean b =null;
	nullRet1 = bool2num(b);
	nullRet2 = bool2num(null);
	return 0;
}