integer i;
integer j; 
integer field; 
integer nullValue; 
integer varWithInitializer = 123;
integer varWithNullInitializer = null;

function integer transform() {
	i=0;
	print_err(i);

	j=-1;
	print_err(j);
	
	field=$0.Value;
	print_err(field);
	
	nullValue = null;
	print_err(nullValue);

	print_err(varWithInitializer);
	print_err(varWithNullInitializer);

	return 0;
}