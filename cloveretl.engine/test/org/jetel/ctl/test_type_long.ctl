long i; 
long j;
long field; 
long def; 
long nullValue;
long varWithInitializer = 123L;
long varWithNullInitializer = null;

function integer transform() {
	i=0;
	printErr(i);
	
	j=-1;
	printErr(j);
 
	field=$0.BornMillisec;
	printErr(field);
	 
	printErr(def); 
	
	nullValue = null;
	printErr(nullValue);
	
	printErr(varWithInitializer);
	printErr(varWithNullInitializer);
	return 0;
}