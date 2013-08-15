integer parsedInteger1;
integer parsedInteger2;
integer parsedInteger3;
integer parsedInteger4;
integer nullRet1;
integer nullRet2;
integer nullRet3;
integer nullRet4;
integer nullRet5;
integer nullRet6;
integer nullRet7;
integer nullRet8;
integer nullRet9;
integer nullRet10;
integer nullRet11;
integer nullRet12;
integer nullRet13;
integer nullRet14;
integer nullRet15;
integer nullRet16;
integer nullRet17;
integer nullRet18;
integer nullRet19;

function integer transform() {
	parsedInteger1 = str2integer("123456789");
	parsedInteger2 = str2integer("$123,123 Millions", "$###,###.# Millions");
	parsedInteger3 = str2integer("-350000 Kc", "#.# Kc", "cs.CZ");
	parsedInteger4 = str2integer("JK", 21);
	string s = null;
	integer i = null;
	nullRet1 = str2integer('123 Mil','### Mil', null);
	nullRet2 = str2integer('123 Mil','### Mil', s);
	nullRet3 = str2integer(null);
	nullRet4 = str2integer(s);
	nullRet5 = str2integer(null, '###');
	nullRet6 = str2integer(s, '###');
	nullRet7 = str2integer(null, s);
	nullRet8 = str2integer(null,'##','cs.CZ');
	nullRet9 = str2integer(s, '##', 'en.US');
	nullRet18 = str2integer(null, 21);
//	nullRet19 = str2integer(null, i); //somehow doesn't work - try after change
	//CLO-1614
//	nullRet10 = str2integer('123', null);
//	nullRet11 = str2integer('123', s);
//	nullRet12 = str2integer('123', null, 'cs.CZ');
//	nullRet13 = str2integer('123', s, 'en.US');
//	nullRet14 = str2integer('123', null, null);
//	nullRet15 = str2integer('123', null, s);
//	nullRet16 = str2integer('123', s, null);
//	nullRet17 = str2integer('123', s, s);
	return 0;
}