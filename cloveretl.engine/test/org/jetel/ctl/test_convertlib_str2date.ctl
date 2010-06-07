date date1;
date date2;
date date3;
date date4;

function integer transform() {
	date1 = str2date('19.May♫2050','dd.MMM♫yyyy','en.US');
	date2 = str2date('2050.May19','yyyy.MMMdd','en.US', true);
	date3 = str2date('19.V2050','dd.MMMyyyy','cs.CZ');
	date4 = str2date('19☺V2050','dd☺MMMyyyy','cs.CZ', true);
	return 0;
}