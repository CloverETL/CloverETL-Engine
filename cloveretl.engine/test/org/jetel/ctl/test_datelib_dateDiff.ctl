date born;
date otherDate;
long ddiff;
long ddiffMilliseconds;
long ddiffSeconds;
long ddiffMinutes;
long ddiffHours;
long ddiffDays;
long ddiffWeeks;
long ddiffMonths;
long ddiffYears;

function integer transform() {
	born=$firstInput.Born;
	otherDate=today();
	
	ddiff=dateDiff(born,otherDate,year);
	printErr('date diffrence:'+ddiff );
	printErr('born: '+born+' otherDate: '+otherDate);

	date before = str2date("12.6.2008", "dd.MM.yyyy");
	date after = str2date("12.6.2009", "dd.MM.yyyy");
	
	ddiffMilliseconds = dateDiff(after,before,millisec);
	ddiffSeconds = dateDiff(after,before,second);
	ddiffMinutes = dateDiff(after,before,minute);
	ddiffHours = dateDiff(after,before,hour);
	ddiffDays = dateDiff(after,before,day);
	ddiffWeeks = dateDiff(after,before,week);
	ddiffMonths = dateDiff(after,before,month);
	ddiffYears = dateDiff(after,before,year);

	return 0;
}