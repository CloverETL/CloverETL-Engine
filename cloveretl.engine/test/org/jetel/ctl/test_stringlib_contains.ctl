string input;

boolean contains1;
boolean contains2;
boolean contains3;
boolean contains4;

boolean indexOf1;
boolean indexOf2;
boolean indexOf3;
boolean indexOf4;

boolean self;

boolean contains_empty1;
boolean contains_empty2;

boolean indexOf_empty1;
boolean indexOf_empty2;

boolean contains_null1;
boolean contains_null2;

boolean indexOf_null1;
boolean indexOf_null2;

function integer transform() {
	input='hello world';
	
	contains1 = contains(input,'l');
	contains2 = contains(input,'hello');
	contains3 = contains(input, 'k');
	contains4 = contains(input, '');
	
	self = contains(input, input);
	
	// check consistency with indexOf()
	indexOf1 = indexOf(input,'l') >= 0;
	indexOf2 = indexOf(input,'hello') >= 0;
	indexOf3 = indexOf(input, 'k') >= 0;
	indexOf4 = indexOf(input, '') >= 0;
	
	contains_empty1 = contains('','a');
	contains_empty2 = contains('','');
	indexOf_empty1 = indexOf('','a') >= 0;
	indexOf_empty2 = indexOf('','') >= 0;
	
	contains_null1 = contains(null, 'a');
	contains_null2 = contains(null, '');
	indexOf_null1 = indexOf(null, 'a') >= 0;
	indexOf_null2 = indexOf(null, '') >= 0;

	return 0;
}