date inputDate;
integer yearDate;
integer monthDate;
integer secondDate;
integer yearBorn;
integer monthBorn;
integer secondBorn;

function integer transform() {
	inputDate = 1987-05-12;
	yearDate = date2num(inputDate, year);
	monthDate = date2num(inputDate, month);
	secondDate = date2num(inputDate, second);
	yearBorn = date2num($firstInput.Born, year);
	monthBorn = date2num($firstInput.Born, month);
	secondBorn = date2num($firstInput.Born, second);
	return 0;
}

	