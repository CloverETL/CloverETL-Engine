string sdate1;
string sdate2;

date date01;
date date02;
date date03;
date date04;
date date11;
date date12;
date date13;
date date21;

function integer transform() {

	date td = today();
	string i2 = "yyyy MMM dd"; 

	for (integer i = 0; i < 3; i++) {	
		sdate1 = date2str(td, i2);
		sdate2 = date2str(td, "yyyy MMM dd");
	}
	
	string ds1 = '20Jul2000';
	string ds2 = 'ddMMMyyyy';
	string ds3 = 'en.GB';
	
	for (integer i = 0; i < 3; i++) {
		date01 = str2date(ds1, ds2, ds3);
		date02 = str2date('20Jul2000',ds2,ds3);
		date03 = str2date('20Jul2000','ddMMMyyyy',ds3);
		date04 = str2date('20Jul2000','ddMMMyyyy','en.GB');

		date11 = str2date(ds1, ds2);
		date12 = str2date('20Jul2000',ds2);
		date13 = str2date('20Jul2000','ddMMMyyyy');
	}
	
	
	return OK;
	
}
