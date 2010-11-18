string inputDate;
string bornDate;
string czechBornDate;
string englishBornDate;

function integer transform() {
	date input = 1987-05-12;
	inputDate = date2str(input, 'yyyy:MM:dd');
	bornDate = date2str($firstInput.Born, 'yyyy:MM:dd'); 
	czechBornDate = date2str($firstInput.Born, 'yyyy:MMMM:dd', 'cs.CZ'); 
	englishBornDate = date2str($firstInput.Born, 'yyyy:MMMM:dd', 'en'); 
	return 0;	
}