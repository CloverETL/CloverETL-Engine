decimal i; 
decimal j; 

decimal field; 
decimal def; 
decimal nullValue;
decimal varWithInitializer = 123.35D;
decimal varWithNullInitializer = null;
decimal varWithInitializerNoDist = 123.35;

function integer transform() {
	i=0;
	print_err(i);
	 
	j=-1.0;
	print_err(j);
	
	field=$0.Currency;
	print_err(field);
	 
	print_err(def);
	 
	nullValue = null;
	print_err(nullValue);
	
	print_err(varWithInitializer);
	print_err(varWithNullInitializer);
	print_err(varWithInitializer);
	
	return 0;
}