date d3; 
date d2; 
date d1; 
date field; 
date nullValue; 
date minValue;
date varWithNullInitializer = null;

function integer transform() {
	d3 = 2006-08-01;
	printErr(d3);
 
	d2 = 2006-08-02 15:15:03;
	printErr(d2);
	 
	d1 = 2006-1-1 1:2:3;
	printErr(d1);
	 
	field = $0.Born;
	printErr(field);
 
	nullValue = null;
	printErr(nullValue);
	 
	minValue = 1970-01-01 01:00:00;
	printErr(minValue);
	printErr(varWithNullInitializer);

	return 0;
}