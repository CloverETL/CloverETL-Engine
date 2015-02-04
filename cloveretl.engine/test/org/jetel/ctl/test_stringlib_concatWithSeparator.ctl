string result1;
string result2;
string result3;
string result4;

string blank;
string variables;

string oneElement;
string noElements;

function integer transform() {
	result1 = concatWithSeparator(",", "a", "b", "c");
	result2 = concatWithSeparator(", ", "a", "b", "c");
	result3 = concatWithSeparator("", "a", "b");
	result4 = concatWithSeparator(", ", "x", " ", "\t", "y", null, "z");

	blank = concatWithSeparator(",", null, " ");

	string a = "a";
	string b = "b";
	string c = null;
	variables = concatWithSeparator(", ", a, b, null, c, "c");
	
	oneElement = concatWithSeparator(",", "a");
	//noElements = concatWithSeparator(",");

	return 0;
}