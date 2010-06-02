integer i;
integer j; 
integer field; 
integer nullValue; 
integer varWithInitializer = 123;
integer varWithNullInitializer = null;

function integer transform() {
	i=0;
	printErr(i);

	j=-1;
	printErr(j);
	
	field=$0.Value;
	printErr(field);
	
	nullValue = null;
	printErr(nullValue);

	printErr(varWithInitializer);
	printErr(varWithNullInitializer);

	return 0;
}