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

string literalParserTest1 = "some // string";
string literalParserTest2 = "some /* string */";
string literalParserTest3 = "some \" string";
string literalParserTest4 = "some \\ string";
string literalParserTest5 = "some \\\" string";
string literalParserTest6 = "some \\\\\" string";
string literalParserTest7 = "some \\\\\\\" string";


function integer transform() {
	i="0";
	printErr(i);
	 
	helloEscaped='hello\nworld';
	printErr(helloEscaped); 
	
	helloExpanded="hello\nworld";
	printErr(helloExpanded);
	 
	fieldName=$Name;
	printErr(fieldName);
 
	fieldCity=$City;
	printErr(fieldCity);
 
	escapeChars='a\u0101\u0102A';
	printErr(escapeChars);
 
	doubleEscapeChars='a\\u0101\\u0102A';
	printErr(doubleEscapeChars); 
	specialChars='špeciálne značky s mäkčeňom môžu byť';
	printErr(specialChars);
	 
	dQescapeChars="a\u0101\u0102A";
	printErr(dQescapeChars);
	 
	dQdoubleEscapeChars="a\\u0101\\u0102A";
	printErr(dQdoubleEscapeChars);
	 
	dQspecialChars="špeciálne značky s mäkčeňom môžu byť";
	printErr(dQspecialChars);
	 
	printErr(empty+specialChars);
	printErr(def); 
	
	nullValue = null;
	printErr(nullValue);
	
	printErr(varWithNullInitializer);
	return 0;
}