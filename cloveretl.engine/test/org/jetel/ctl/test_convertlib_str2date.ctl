date date1;
date date2;
date date3;
date withTimeZone1;
date withTimeZone2;
date nullRet1;
date nullRet2;
date nullRet3;
date nullRet4;
date nullRet5;
date nullRet6;


function integer transform() {
	date1 = str2date('2005-19-11', 'yyyy-dd-MM');
	date2 = str2date('19.May♫2050','dd.MMM♫yyyy','en.US');
	date3 = str2date('19.V2050','dd.MMMyyyy','cs.CZ');
	withTimeZone1 = str2date('30.5.2013 17:15:12', 'dd.MM.yyyy HH:mm:ss', 'en.US', 'GMT+8');
	withTimeZone2 = str2date('30/5/2013 17:15:12', 'dd/MM/yyyy HH:mm:ss', 'cs.CZ', 'GMT-8');
	string str = null;
	nullRet1 = str2date(str, 'yyyy-dd-MM');
	nullRet2 = str2date(null, 'yyyy-MM-dd');
	nullRet3 = str2date(str, 'yyyy-MM-dd', 'cs.CZ');
	nullRet4 = str2date(null, 'yyyy-dd-MM', 'en.US');
	nullRet5 = str2date(str, 'yyyy-dd-MM', 'cs.CZ','GMT+8');
	nullRet6 = str2date(null, 'yyyy-dd-MM', 'en.US', 'GMT+2');
	return 0;
}