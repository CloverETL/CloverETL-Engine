string[] chars;
string input;
string input2;
string input3;
string input4;

string test_empty;
string test_null;

function integer transform() {
	input='The QUICk !!$  broWn fox 	juMPS over the lazy DOG	'; 
	for (integer i = 0; i < length(input); i++) {
		chars[i] = (charAt(input,i));
	};
//	input2 = charAt('milk', 7);
//	input3 = charAt('milk', '');
//	input4 = charAt('milk', null);
//	test_empty = charAt('', 1);
//	test_null = charAt(null, 1);
	
	return 0;
}