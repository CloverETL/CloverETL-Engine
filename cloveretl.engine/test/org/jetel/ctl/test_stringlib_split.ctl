string input;

string[] split1;
string[] split2;
string[] split3;

string[] limit1;
string[] limit2;
string[] limit3;
string[] limit4;
string[] limit20;

string[] test_empty;
string[] test_empty2;
string[] test_empty3;
string[] test_empty4;
string[] test_empty5;
string[] test_empty6;

string[] test_null;

string[] removeTrailing1;
string[] removeTrailing2;
string[] removeTrailing3;
string[] removeTrailing4;

string[] keepTrailing1;
string[] keepTrailing2;


function integer transform() {
	input = 'The quick brown fox jumps over the lazy dog';
	split1 = split(input, '[ox]');
	split2 = split(input, '[ox]', 0);
	split3 = split(input, '[ox]', -1);
	
	limit1 = split(input, '[ox]', 1);
	limit2 = split(input, '[ox]', 2);
	limit3 = split(input, '[ox]', 3);
	limit4 = split(input, '[ox]', 4);
	limit20 = split(input, '[ox]', 20);
	
	test_empty = split('',';');
	test_empty2 = split('aa','');
	test_empty3 = split('',';', 0);
	test_empty4 = split('aa','', 0);
	test_empty5 = split('',';', -1);
	test_empty6 = split('aa','', -1);

	test_null = split(null,'a');
	
	removeTrailing1 = split(';;;;', ';');
	removeTrailing2 = split(';;;;', ';', 0);
	removeTrailing3 = split(';a;;b;;;', ';');
	removeTrailing4 = split(';a;;b;;;', ';', 0);
	
	keepTrailing1 = split(';;;;', ';', -1);
	keepTrailing2 = split(';a;;b;;;', ';', -10);
	
	return 0;
}