date date1;
date date2;
date date3;
date date3d;
date date4;
date date4d;
date date5;
date date6;
date date7;
date date7d;
date date8;
date date8d;
date date9;
date date10;

boolean b11;
boolean b12;
boolean b21;
boolean b22;
boolean b31;
boolean b32;
boolean b41;
boolean b42;


function integer transform() {

	for (integer i = 0; i < 3; i++) {
	
		setRandomSeed(-1);
		date1 = randomDate("2009.01.01","2009.12.31","yyyy.MM.dd");
		
	
		date1 = randomDate(2010-02-02, 2010-05-05);
		b11 = date1 >= 2010-02-02;
		b12 = date1 <= 2010-05-05;
		 
		date2 = randomDate(10000000l, 20000000l);
		b21 = date2 >= long2date(10000000l);
		b22 = date2 <= long2date(20000000l);
		
		setRandomSeed(20001);
		date3 = randomDate(2010-02-02, 2010-05-05);
		setRandomSeed(20001);
		date3d = randomDate(2010-02-02, 2010-05-05);
		
		setRandomSeed(20001);
		date4 = randomDate(10000000l, 20000000l);
		setRandomSeed(20001);
		date4d = randomDate(10000000l, 20000000l);
		
		date5 = randomDate("20100110", "20100120", "yyyymmdd");
		b31 = date5 >= 2010-01-10;
		b32 = date5 <= 2010-01-20;
		
		date6 = randomDate("20100110", "20100120", "yyyymmdd", "en.GB");
		b41 = date5 >= 2010-01-10;
		b42 = date5 <= 2010-01-20;		
		
		setRandomSeed(20001);
		date7 = randomDate("20100110", "20100120", "yyyymmdd");
		setRandomSeed(20001);
		date7d = randomDate("20100110", "20100120", "yyyymmdd");
		
		setRandomSeed(20001);
		date8 = randomDate("20100110", "20100120", "yyyymmdd", "en.GB");
		setRandomSeed(20001);
		date8d = randomDate("20100110", "20100120", "yyyymmdd", "en.GB");
	}
	
	return OK;
	
}
