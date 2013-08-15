double parsedDouble1;
double parsedDouble2;
double parsedDouble3;
number nullRet1;
number nullRet2;
number nullRet3;
number nullRet4;
number nullRet5;
number nullRet6;
number nullRet7;
number nullRet8;
number nullRet9;
number nullRet10;
number nullRet11;
number nullRet12;
number nullRet13;
number nullRet14;
number nullRet15;

function integer transform() {
	parsedDouble1 = str2double("100.13");
	parsedDouble2 = str2double("$123,123,123.123 Millions", "$###,###.# Millions");
	parsedDouble3 = str2double("-350000,01 Kc", "#.# Kc", "cs.CZ");
	string num = null;
	string s = null;
	nullRet1 = str2double(num);
	nullRet2 = str2double(null);
	nullRet3 = str2double(num, '###,# Mil');
	nullRet4 = str2double(null, '###,# CZK');
	nullRet5 = str2double(num,'###,# Mil', 'en.US');
	nullRet6 = str2double(null, '###,# CZK', 'cs.CZ');
	nullRet7 = str2double('12.34', '##.##', null);
	//CLO-1614
//	nullRet8 = str2double('12.34', null);
//	nullRet9 = str2double('12.34', s);
//	nullRet10 = str2double('12.34', null, null);
//	nullRet11 = str2double('12.34', s, null);
//	nullRet12 = str2double('12.34', null, s);
//	nullRet13 = str2double('12.34', s, s);
//	nullRet14 = str2double('12.34', s, 'cs.CZ');
//	nullRet15 = str2double('12.34', null, 'en.US');	
	return 0;
}