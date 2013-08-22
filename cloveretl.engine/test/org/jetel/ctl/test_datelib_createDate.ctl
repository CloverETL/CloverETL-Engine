date date1;
date date2;
date date3;
date dateTime1;
date dateTime2;
date dateTime3;
date dateTimeMillis1;
date dateTimeMillis2;
date dateTimeMillis3;
date ret1;
date ret2;
date ret3;
date ret4;
date ret5;

function integer transform() {

	// no time zone
	date1 = createDate(2013, 6, 11);
	dateTime1 = createDate(2013, 6, 11, 14, 27, 53);
	dateTimeMillis1 = createDate(2013, 6, 11, 14, 27, 53, 123);
	
	// literal
	date2 = createDate(2013, 6, 11, "GMT+5");
	dateTime2 = createDate(2013, 6, 11, 14, 27, 53, "GMT+5");
	dateTimeMillis2 = createDate(2013, 6, 11, 14, 27, 53, 123, "GMT+5");
	
	// variable
	string timeZone = "GMT+5";
	date3 = createDate(2013, 6, 11, timeZone);
	dateTime3 = createDate(2013, 6, 11, 14, 27, 53, timeZone);
	dateTimeMillis3 = createDate(2013, 6, 11, 14, 27, 53, 123, timeZone);

	string str = null;
	//CLO-1674
//	ret1 = createDate(2011, 11, 20, null);
//	ret5 = createDate(2011, 11, 20, 4, 20, 11, 12, null);
	ret2 = createDate(2011, 11, 20, str);
	ret3 = createDate(2011, 11, 20, 4, 20, 11, str);
	ret4 = createDate(2011, 11, 20, 4, 20, 11, 123, str);
	return 0;
}