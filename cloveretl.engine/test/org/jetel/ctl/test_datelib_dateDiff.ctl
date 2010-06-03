date born;
date otherDate;
integer ddiff;

function integer transform() {
	born=$firstInput.Born;
	otherDate=today();
	
	ddiff=dateDiff(born,otherDate,year);
	printErr('date diffrence:'+ddiff );
	printErr('born: '+born+' otherDate: '+otherDate);
	return 0;
}