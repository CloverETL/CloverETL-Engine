date date1;
date date2;
date date3;
date dateTime1;
date dateTime2;
date dateTime3;
date dateTimeMillis1;
date dateTimeMillis2;
date dateTimeMillis3;

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

	return 0;
}