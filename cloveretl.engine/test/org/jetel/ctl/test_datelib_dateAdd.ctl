date born;
date datum;

function integer transform() {
	born=$firstInput.Born;
	printErr('born=' + born);
	
	datum=dateAdd(born,100,millisec);
	printErr('datum = ' + datum );
	
	return 0;
}

