date inputDate;
date minDate;
integer yearDate;
integer monthDate;
integer secondDate;
integer yearBorn;
integer monthBorn;
integer secondBorn;
integer yearMin;
integer monthMin;
integer weekMin;
integer weekMinCs;
integer dayMin;
integer hourMin;
integer minuteMin;
integer secondMin;
integer millisecMin;
integer nullRet1;
integer nullRet2;

integer week1;
integer week2;

function integer transform() {
	inputDate = 1987-05-12;
	minDate = zeroDate();
	yearDate = date2num(inputDate, year);
	monthDate = date2num(inputDate, month);
	secondDate = date2num(inputDate, second);
	yearBorn = date2num($firstInput.Born, year);
	monthBorn = date2num($firstInput.Born, month);
	secondBorn = date2num($firstInput.Born, second);
	yearMin = date2num(minDate, year);
	monthMin = date2num(minDate, month);
	weekMin = date2num(minDate, week);
	weekMinCs = date2num(minDate, week, "cs");
	dayMin = date2num(minDate, day);
	hourMin = date2num(minDate, hour);
	minuteMin = date2num(minDate, minute);
	secondMin = date2num(minDate, second);
	millisecMin = date2num(minDate, millisec);
	nullRet1 = date2num(null,second);
	nullRet2 = date2num(null,minute, "sk");

	week1 = date2num(long2date(1391900400000L), week, "cs.CZ");
	week2 = date2num(long2date(1391986800000L), week, "cs.CZ");
	
	return 0;
}

	