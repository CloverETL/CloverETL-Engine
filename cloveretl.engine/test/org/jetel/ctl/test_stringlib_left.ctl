string input;
string lef;
string padded;
string notPadded;

function integer transform() {
	input = "The quick brown fox jumps over the lazy dog";
	lef=left(input,5);
	
	padded = left(lef,8,true);
	notPadded = left(lef,8,false);
	
	return 0;
}