string input;
string righ;
string rightPadded;
string rightNotPadded;
string padded;
string notPadded;
string short;
string shortPadded;
string shortNotPadded;

function integer transform() {
	input = "The quick brown fox jumps over the lazy dog";
	righ=right(input,5);
	rightNotPadded = right(input,5, false);
	rightPadded = right(input,5, true);
	
	padded = right(righ,8,true);
	notPadded = right(righ,8,false);
	input = "Dog";
	short = right(input, 8);
	shortNotPadded = right(input, 8, false);
	shortPadded = right(input, 8, true); 
	return 0;
}