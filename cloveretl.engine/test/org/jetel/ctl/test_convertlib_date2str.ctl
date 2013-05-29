string inputDate;
string bornDate;
string czechBornDate;
string englishBornDate;
string[] loopTest;

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
	
	return 0;	
}