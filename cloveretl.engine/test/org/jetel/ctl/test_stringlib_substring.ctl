string input;
string subs;
string test1;
string test_empty;
string nullLiteral;
string nullVariable;

string subs2;
string test2;
string test3;
string test_empty2;
string nullLiteral2;
string nullVariable2;

string result1;
string result2;
string result3;
string result4;
string result5;
string result6;
string result7;
string result8;
string result9;
string result10;
string result11;
string result12;

function integer transform() {
	input='The QUICk !!$  broWn fox 	juMPS over the lazy DOG	';
	subs=substring(input, 5, 5);
	test1 = substring('aaa',0,0);
	test_empty = substring('',0,5);
	nullLiteral = substring(null,5,5);
	string nullInput = null;
	nullVariable = substring(nullInput, 5, 5);
	
	subs2 = substring(input, 5);
	test2 = substring('aaa',0);
	test3 = substring('aaa',3);
	test_empty2 = substring('',0);
	nullLiteral2 = substring(null,5);
	nullVariable2 = substring(nullInput, 5);
	
	string abc = "abcdefghi";
	
	result1 = abc.substring(0); // zero beginIndex
	result2 = abc.substring(5);
	result3 = abc.substring(abc.length());
	result4 = abc.substring(abc.length() + 1);
	result5 = abc.substring(1000);
	result6 = abc.substring(0, abc.length());
	result7 = abc.substring(5, 0);
	result8 = abc.substring(5, 1);
	result9 = abc.substring(5, abc.length());
	result10 = abc.substring(abc.length(), 1);
	result11 = abc.substring(abc.length() - 1);
	result12 = abc.substring(abc.length() - 1, 1);
	
	return 0;
}