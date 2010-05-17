boolean b1; 
boolean b2; 
boolean b3;
boolean nullValue;
boolean varWithNullInitializer = null;

function integer transform() {
	b1=true;
	print_err(b1);
	 
	b2=false;
	print_err(b2);
 
	print_err(b3);
	
	nullValue = null;
	print_err(nullValue);
	print_err(varWithNullInitializer);
	
	return 0;
}