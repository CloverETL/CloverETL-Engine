string input;
string righ;
string padded;
string notPadded;

function integer transform() {
	input = "The quick brown fox jumps over the lazy dog";
	righ=right(input,5);
	
	padded = right(righ,8,true);
	notPadded = right(righ,8,false);

	return 0;
}