date date1;
date date2;
date withTimeZone1;
date withTimeZone2;

function integer transform() {
	date1 = str2date('19.May♫2050','dd.MMM♫yyyy','en.US');
	date2 = str2date('19.V2050','dd.MMMyyyy','cs.CZ');
	withTimeZone1 = str2date('30.5.2013 17:15:12', 'dd.MM.yyyy HH:mm:ss', 'en.US', 'GMT+8');
	withTimeZone2 = str2date('30/5/2013 17:15:12', 'dd/MM/yyyy HH:mm:ss', 'cs.CZ', 'GMT-8');
	return 0;
}