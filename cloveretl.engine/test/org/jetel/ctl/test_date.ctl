date d3; 
date d2; 
date d1; 
date field; 
date nullValue; 
date minValue;
date varWithNullInitializer = null;

function integer transform() {
	d3 = 2006-08-01;
	print_err(d3);
 
	d2 = 2006-08-02 15:15:03;
	print_err(d2);
	 
	d1 = 2006-1-1 1:2:3;
	print_err(d1);
	 
	field = $0.Born;
	print_err(field);
 
	nullValue = null;
	print_err(nullValue);
	 
	minValue = 1970-01-01 01:00:00;
	print_err(minValue);
	print_err(varWithNullInitializer);

	return 0;
}