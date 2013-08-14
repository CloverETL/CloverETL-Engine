integer cond1;
boolean res11; 
boolean res12; 
boolean res13;
integer cond2;
boolean res21; 
boolean res22; 
boolean res23;
integer cond3;
boolean res31; 
boolean res32; 
boolean res33;
integer cond4;
boolean res41; 
boolean res42; 
boolean res43;
integer cond5;
boolean res51; 
boolean res52; 
boolean res53;
boolean res54; 
integer cond6;
boolean res61; 
boolean res62; 
boolean res63;
boolean res64; 
string res71;
string res72;
string res73;
string res74;


function integer transform() {
	// simple switch
	cond1 = 1;
	res11 = false; 
	res12 = false; 
	res13 = false;
	switch (cond1) {
	case 0: res11 = true;
		break; 
	case 1: res12 = true;
		break; 
	default: res13 = true;
		break; 
	} 

	// empty integer switch
	switch (cond1) {
	}

	// integer switch with empty case
	switch (cond1) {
	case 0:
	}

	// integer switch with one case and a break
	switch (cond1) {
	case 0:
		break;
	}
	
	string stringCond = "a";
	// empty string switch
	switch (stringCond) {
	}

	// string switch with empty case
	switch (stringCond) {
	case "b":
	}

	// string switch with one case and a break
	switch (stringCond) {
	case "b":
		break;
	}

	// simple switch, no break
	cond2 = 1;
	res21 = false; 
	res22 = false; 
	res23 = false;
	switch (cond2) {
	case 0: res21 = true;
		break; 
	case 1: res22 = true;
	default: res23 = true;
		break; 
	}
	// default branch
	cond3 = 3;
	res31 = false; 
	res32 = false; 
	res33 = false;
	switch (cond3) {
	case 0: res31 = true;
		break; 
	case 1: res32 = true;
		break; 
	default: res33 = true;
		break; 
	}
	// no-default branch => no match
	cond4 = 3;
	res41 = false; 
	res42 = false; 
	res43 = false;
	switch (cond4) {
	case 0: res41 = true;
		break; 
	case 1: res42 = true;
		break; 
	}
	// multiple statements under single case
	cond5 = 1;
	res51 = false; 
	res52 = false; 
	res53 = false;
	res54 = false; 
	switch (cond5) {
	case 0: res51 = true;
		break; 
	case 1: 
		res52 = true;
		res53 = true;
		break; 
	default: res54 = true;
		break; 
	}
	// single statement for multiple cases
	cond6 = 1;
	res61 = false; 
	res62 = false; 
	res63 = false;
	boolean res64 = false; 
	switch (cond6) {
	case 0:
		res61 = true;
	case 1:
	case 2: 
		res62 = true;
	case 3: 
	case 4: 
		res63 = true;
		break; 
	default: res64 = true;
		break; 
	}
	
	// this tests if the variable stack is restored after a switch with no break
	string s = "something";
	
	switch (s) {
		case "nothing":
	}
	
	s = "something";

	switch (s) {
		case "something":
	}
	
	s = "something";
	
	
	// null cases
	switch (s) {
		case null:
			res71 = "null case";
			break;
		default:
			res71 = "default case";
			break;
	}

	string cond7 = null;
	switch (cond7) {
		case null:
			res72 = "null case";
			break;
		default:
			res72 = "default case";
			break;
	}
	
	integer cond8 = null;
	switch (cond8) {
		case null:
			res73 = "null case";
			break;
		default:
			res73 = "default case";
			break;
	}

	cond8 = 1;
	switch (cond8) {
		case null:
			res74 = "null case";
			break;
		default:
			res74 = "default case";
			break;
	}
	
	return 0;
}