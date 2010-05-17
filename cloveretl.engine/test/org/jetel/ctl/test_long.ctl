long i; 
long j;
long field; 
long def; 
long nullValue;
long varWithInitializer = 123L;
long varWithNullInitializer = null;

function integer transform() {
	i=0;
	print_err(i);
	
	j=-1;
	print_err(j);
 
	field=$0.BornMillisec;
	print_err(field);
	 
	print_err(def); 
	
	nullValue = null;
	print_err(nullValue);
	
	print_err(varWithInitializer);
	print_err(varWithNullInitializer);
	return 0;
}