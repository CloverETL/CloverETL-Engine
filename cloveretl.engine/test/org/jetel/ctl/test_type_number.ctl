number i; 
number j;
number field; 
number def; 
number nullValue;
number varWithNullInitializer = null;

function integer transform() {
	i=0;
	printErr(i); 

	j=-1.0;
	printErr(j);
	 
	field = $0.Age;
	printErr(field); 
	printErr(def);
	
	nullValue = null;
	printErr(nullValue);
	printErr(varWithNullInitializer);
	
	return 0;
}