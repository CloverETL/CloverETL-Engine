string input;
string righ;
string rightPadded;
string rightNotPadded;
string padded;
string notPadded;
string short;
string shortPadded;
string shortNotPadded;
string simple;

string test_null1;
string test_null2;
string test_null3;

string test_empty1;
string test_empty2;
string test_empty3;
function integer transform() {
	input = "The quick brown fox jumps over the lazy dog";
	righ=right(input,5);
	rightNotPadded = right(input,5, false);
	rightPadded = right(input,5, true);
	simple = right('milk',9);
	padded = right(righ,8,true);
	notPadded = right(righ,8,false);
	input = "Dog";
	short = right(input, 8);
	shortNotPadded = right(input, 8, false);
	shortPadded = right(input, 8, true);
	
	test_null1 = right(null,4);
	test_null2 = right(null,4,false);
	test_null3 = right(null,2,true);
	
	test_empty1 = right('',5);
	test_empty2 = right('',4,false);
	test_empty3 = right('',3,true);
	return 0;
}