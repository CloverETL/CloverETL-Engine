boolean b1; 
boolean b2; 
boolean b3;
boolean nullValue;
boolean varWithNullInitializer = null;

function integer transform() {
	b1=true;
	printErr(b1);
	 
	b2=false;
	printErr(b2);
 
	printErr(b3);
	
	nullValue = null;
	printErr(nullValue);
	printErr(varWithNullInitializer);
	
	return 0;
}