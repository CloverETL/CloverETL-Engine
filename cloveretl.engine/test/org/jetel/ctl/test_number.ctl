number i; 
number j;
number field; 
number def; 
number nullValue;
number varWithNullInitializer = null;

function integer transform() {
	i=0;
	print_err(i); 

	j=-1.0;
	print_err(j);
	 
	field = $0.Age;
	print_err(field); 
	print_err(def);
	
	nullValue = null;
	print_err(nullValue);
	print_err(varWithNullInitializer);
	
	return 0;
}