boolean b1;
boolean b2;
string b4;
integer i;
	
function integer transform() {
	b1=true;
	printErr(b1);
	
	b2=false;
	printErr(b2);
	
	b4="hello";
	printErr(b4);
	
	b2 = true;
	printErr(b2);
	
	if (b2) {
		i=2;
		printErr('i: ' + i);
	}
	printErr(b2);
	
	b4=null;
	printErr(b4);
	b4='hi';
	printErr(b4);
	return 0; 
}