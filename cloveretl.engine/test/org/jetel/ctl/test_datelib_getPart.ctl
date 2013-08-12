integer year1;
integer year2;
integer year3;

integer month1;
integer month2;
integer month3;

integer day1;
integer day2;
integer day3;

integer hour1;
integer hour2;
integer hour3;

integer minute1;
integer minute2;
integer minute3;

integer second1;
integer second2;
integer second3;

integer millisecond1;
integer millisecond2;
integer millisecond3;

integer year_null;
integer month_null;
integer day_null;
integer hour_null;
integer minute_null;
integer second_null;
integer milli_null;

integer year_null2;
integer month_null2;
integer day_null2;
integer hour_null2;
integer minute_null2;
integer second_null2;
integer milli_null2;

integer year_null3;
integer month_null3;
integer day_null3;
integer hour_null3;
integer minute_null3;
integer second_null3;
integer milli_null3;

function integer transform() {
	date myDate = str2date("2013-06-11 14:46:34 GMT+1:00", "yyyy-MM-dd HH:mm:ss z");
	myDate = dateAdd(myDate, 123, millisec);
	
	// no timezone
	year1 = getYear(myDate);
	month1 = getMonth(myDate);
	day1 = getDay(myDate);
	hour1 = getHour(myDate);
	minute1 = getMinute(myDate);
	second1 = getSecond(myDate);
	millisecond1 = getMillisecond(myDate);
	
	// literal
	year2 = getYear(myDate, "GMT+5");
	month2 = getMonth(myDate, "GMT+5");
	day2 = getDay(myDate, "GMT+5");
	hour2 = getHour(myDate, "GMT+5");
	minute2 = getMinute(myDate, "GMT+5");
	second2 = getSecond(myDate, "GMT+5");
	millisecond2 = getMillisecond(myDate, "GMT+5");
	
	// variable
	string timeZone = "GMT+5";
	
	year3 = getYear(myDate, timeZone);
	month3 = getMonth(myDate, timeZone);
	day3 = getDay(myDate, timeZone);
	hour3 = getHour(myDate, timeZone);
	minute3 = getMinute(myDate, timeZone);
	second3 = getSecond(myDate, timeZone);
	millisecond3 = getMillisecond(myDate, timeZone);
	
	timeZone = null;
	year_null = getYear(myDate, timeZone);
	month_null = getMonth(myDate, timeZone);
	day_null = getDay(myDate, timeZone);
	hour_null = getHour(myDate, timeZone);
	minute_null = getMinute(myDate, timeZone);
	second_null = getSecond(myDate, timeZone);
	milli_null = getMillisecond(myDate, timeZone);
	
	date d = null;
	timeZone = "GMT+5";
	year_null2 = getYear(d, timeZone);
	month_null2 = getMonth(d, timeZone);
	day_null2 = getDay(d, timeZone);
	hour_null2 = getHour(d, timeZone);
	minute_null2 = getMinute(d, timeZone);
	second_null2 = getSecond(d, timeZone);
	milli_null2 = getMillisecond(d, timeZone);
	
	year_null3 = getYear(null);
	month_null3 = getMonth(null);
	day_null3 = getDay(null);
	hour_null3 = getHour(null);
	minute_null3 = getMinute(null);
	second_null3 = getSecond(null);
	milli_null3 = getMillisecond(null);
	
	return 0;
}