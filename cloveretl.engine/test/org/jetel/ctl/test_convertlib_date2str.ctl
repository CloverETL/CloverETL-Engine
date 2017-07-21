string inputDate;
string bornDate;
string czechBornDate;
string englishBornDate;
string[] loopTest;
string timeZone;
string nullRet;
string nullRet2;
string nullRet3;
string nullRet4;
string nullRet5;

function integer transform() {
	date input = 1987-05-12;
	inputDate = date2str(input, 'yyyy:MM:dd');
	bornDate = date2str($firstInput.Born, 'yyyy:MM:dd'); 
	czechBornDate = date2str($firstInput.Born, 'yyyy:MMMM:dd', 'cs.CZ'); 
	englishBornDate = date2str($firstInput.Born, 'yyyy:MMMM:dd', 'en'); 
	
	// locale must not be a literal for the test to work
	string[] locales = ['en', 'pl', null, 'cs.CZ', null];
	for (integer i = 0; i < locales.length(); i++) {
		loopTest.push(date2str($firstInput.Born, 'yyyy:MMMM:dd', locales[i]));
	} 
	
	timeZone = date2str($firstInput.Born, 'yyyy:MMMM:dd z', 'en', 'GMT+8');
	nullRet = date2str(null,'yyyy:MM:dd', 'en', 'GMT+8');
	date d = null;
	nullRet2 = date2str(d,'yyyy:MM:dd', 'en', 'GMT+8');
	nullRet3 = date2str(2011-4-15, null);
	nullRet4 = date2str(2011-4-15, null, null);
	nullRet5 = date2str(2011-4-15, null, null, null);
	printErr(nullRet3);
	
	return 0;	
}