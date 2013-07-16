string input;
string[] split1;
string[] test_empty;
string[] test_empty2;
string[] test_null;
function integer transform() {
	input = 'The quick brown fox jumps over the lazy dog';
	split1 = split(input, '[ox]');
	test_empty = split('',';');
	test_empty2 = split('aa','');
	test_null = split(null,'a');
	return 0;
}