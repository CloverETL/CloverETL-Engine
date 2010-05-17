boolean b1;
boolean b2;
string b4;
integer i;
	
function integer transform() {
	b1=true;
	print_err(b1);
	
	b2=false;
	print_err(b2);
	
	b4="hello";
	print_err(b4);
	
	b2 = true;
	print_err(b2);
	
	if (b2) {
		i=2;
		print_err('i: ' + i);
	}
	print_err(b2);
	
	b4=null;
	print_err(b4);
	b4='hi';
	print_err(b4);
	return 0; 
}