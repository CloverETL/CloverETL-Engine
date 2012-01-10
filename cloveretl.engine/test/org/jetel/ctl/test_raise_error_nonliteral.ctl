// This code is valid - raise_error is a valid function termination point 

function integer validate(integer a) {
	switch (a) {
		case 0:
			raiseError("Ship time can't be lower than order date");
		case 1:
			raiseError("Ship time can't be lower than order date: " + 1);
		case 2:
			raiseError("Ship time can't be lower than order date: " + a);
		case 3:
			raiseError("Ship time can't be lower than order date: " + "illegal value");
		case 4:
			raiseError("Ship time can't be lower than order date: " + "illegal value: " + 4);
	}
	return a;
}

function integer transform(){
	integer a = 5;
	return validate(a);
}