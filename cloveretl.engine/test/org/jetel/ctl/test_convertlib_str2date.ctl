date date1;
date date2;

function integer transform() {
	date1 = str2date('19.May♫2050','dd.MMM♫yyyy','en.US');
	date2 = str2date('19.V2050','dd.MMMyyyy','cs.CZ');
	return 0;
}