string input;
string lef;
string padded;
string notPadded;
string lef2;

function integer transform() {
	input = "The quick brown fox jumps over the lazy dog";
      lef=left(input,5);
      lef2=left(input,500);
	
	padded = left(lef,8,true);
	notPadded = left(lef,8,false);
	
	return 0;
}