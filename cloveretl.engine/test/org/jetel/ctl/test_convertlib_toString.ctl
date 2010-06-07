string integerString;
string longString;
string doubleString;
string decimalString;
string listString;
string mapString;

function integer transform() {
	integer integerToString = 10; 
	long longToString = 110654321874L; 
	double doubleToString = 0.00000000000001547874; 
	decimal decimalToString = -6847521431.1545874d;
	string[] listToString = ["not ALI A", "not ALI B", "not ALI D...", "but", "ALI H!"];
	map[integer, string] mapToString;
	mapToString[1] =  "Testing";
	mapToString[2] = "makes";
	mapToString[3] = "me";
	mapToString[4] = "crazy :-)";
	integerString = toString(integerToString);
	longString = toString(longToString);
	doubleString = toString(doubleToString);
	decimalString = toString(decimalToString);
	listString = toString(listToString);
	mapString = toString(mapToString);
	return 0;
}