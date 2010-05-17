string i; 
string helloEscaped; 
string helloExpanded; 
string fieldName; 
string fieldCity; 
string escapeChars; 
string doubleEscapeChars; 
string specialChars; 
string dQescapeChars; 
string dQdoubleEscapeChars; 
string dQspecialChars; 
string empty=""; 
string def; 
string nullValue;
string varWithNullInitializer = null;

function integer transform() {
	i="0";
	print_err(i);
	 
	helloEscaped='hello\nworld';
	print_err(helloEscaped); 
	
	helloExpanded="hello\nworld";
	print_err(helloExpanded);
	 
	fieldName=$Name;
	print_err(fieldName);
 
	fieldCity=$City;
	print_err(fieldCity);
 
	escapeChars='a\u0101\u0102A';
	print_err(escapeChars);
 
	doubleEscapeChars='a\\u0101\\u0102A';
	print_err(doubleEscapeChars); 
	specialChars='špeciálne značky s mäkčeňom môžu byť';
	print_err(specialChars);
	 
	dQescapeChars="a\u0101\u0102A";
	print_err(dQescapeChars);
	 
	dQdoubleEscapeChars="a\u0101\u0102A";
	print_err(dQdoubleEscapeChars);
	 
	dQspecialChars="špeciálne značky s mäkčeňom môžu byť";
	print_err(dQspecialChars);
	 
	print_err(empty+specialChars);
	print_err(def); 
	
	nullValue = null;
	print_err(nullValue);
	
	print_err(varWithNullInitializer);
	return 0;
}