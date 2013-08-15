long parsedLong1;
long parsedLong2;
long parsedLong3;
long parsedLong4;
long nullRet1;
long nullRet2;
long nullRet3;
long nullRet4;
long nullRet5;
long nullRet6;
long nullRet7;
long nullRet8;
long nullRet9;
long nullRet10;
long nullRet11;
long nullRet12;
long nullRet13;
long nullRet14;
long nullRet15;
long nullRet16;
long nullRet17;
long nullRet18;
long nullRet19;

function integer transform() {
	parsedLong1 = str2long("1234567890123");
	parsedLong2 = str2long("$123,123,123456789 Millions", "$###,###.# Millions");
	parsedLong3 = str2long("-350000 Kc", "#.# Kc", "cs.CZ");
	parsedLong4 = str2long("B1", 12);
	string s  = null;
	integer i = null;
	nullRet1 = str2long('123 Mil','### Mil', null);
	nullRet2 = str2long('123 Mil','### Mil', s);
	nullRet3 = str2long(null);
	nullRet4 = str2long(s);
	nullRet5 = str2long(null, '###');
	nullRet6 = str2long(s, '###');
	nullRet7 = str2long(null, s);
	nullRet8 = str2long(null,'##','cs.CZ');
	nullRet9 = str2long(s, '##', 'en.US');
	nullRet18 = str2long(null, 21);
	nullRet19 = str2long(null, i); 
	//CLO-1614
	//nullRet10 = str2long('123', null); // disabled - ambiguous
	nullRet11 = str2long('123', s);
	nullRet12 = str2long('123', null, 'cs.CZ');
	nullRet13 = str2long('123', s, 'en.US');
	nullRet14 = str2long('123', null, null);
	nullRet15 = str2long('123', null, s);
	nullRet16 = str2long('123', s, null);
	nullRet17 = str2long('123', s, s);
	return 0;
}