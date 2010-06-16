double inputDouble;
date inputDate;
number inputNumber;
long truncLong;
date truncDate;
long truncNumber;

function integer transform() {
	inputDouble=-pow(3,1.2);
	inputDate=2004-01-02 17:13:20;
	inputNumber=pi();
	
	truncLong=trunc(inputDouble);	
	truncDate=trunc(inputDate);
	truncNumber=trunc(inputNumber);
	
	printErr('truncation of '+inputDate+'='+truncDate);
	printErr('truncation of '+inputDouble+'='+truncLong);
	printErr('truncation of '+inputNumber+'='+truncNumber);
	return 0;
}