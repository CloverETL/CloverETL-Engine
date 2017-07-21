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
	printErr(i);
	 
	j=-1.0;
	printErr(j);
	
	field=$0.Currency;
	printErr(field);
	 
	printErr(def);
	 
	nullValue = null;
	printErr(nullValue);
	
	printErr(varWithInitializer);
	printErr(varWithNullInitializer);
	printErr(varWithInitializer);
	
	return 0;
}