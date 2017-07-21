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
	
	return 0;
}