long toLong;
long toLong2;
long toLong3;
long nullRet1;
long nullRet2;

function integer transform() {
	decimal decimal1 = 0.007;
	decimal decimal2 = -500.123;
	decimal decimal3 = 10000000000.0;
	toLong = decimal2long(decimal1);
	toLong2 = decimal2long(decimal2);
	toLong3 = decimal2long(decimal3);
	decimal dec = null;
	nullRet1 = decimal2long(dec);
	nullRet2 = decimal2long(null);	
	return 0;
	
}