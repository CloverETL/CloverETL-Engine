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
integer cond6;
boolean res61; 
boolean res62; 
boolean res63;
integer i;
boolean[] res7; 
boolean res71;
boolean res72;
boolean res73; 
string[] res8;

function integer transform() {
	// simple switch using integer
	cond1 = 1;
	res11 = false; 
	res12 = false; 
	res13 = false;
	switch (cond1) {
	case 1:
		res11 = true;
		break; 	
	case 12:
		res12 = true;
		break; 
	default:
		res13 = true;
		break; 
	}
	// first case is not followed by a break
	cond2 = 1;
	res21 = false; 
	res22 = false; 
	res23 = false;
	switch (cond2) {
	case 1:
		res21 = true;
	case 12:
		res22 = true;
		break; 
	default:
		res23 = true;
		break; 
	}
	// first and second case have multiple labels
	cond3 = 12;
	res31 = false; 
	res32 = false; 
	res33 = false;
	switch (cond3) {
	case 10:
	case 11:
		res31 = true;
		break; 	
	case 12:
	case 13:
		res32 = true;
		break; 
	default:
		res33 = true;
		break; 
	}
	// first and second case have multiple labels and no break after first group
	cond4 = 11;
	res41 = false; 
	res42 = false; 
	res43 = false;
	switch (cond4) {
	case 10:
	case 11:
		res41 = true;
	case 12:
	case 13:
		res42 = true;
		break; 
	default:
		res43 = true;
		break; 
	}
	// default case intermixed with other case labels in the second group
	cond5 = 11;
	res51 = false; 
	res52 = false;
	res53 = false;
	switch (cond5) {
	case 10:
	case 11:
		res51 = true;
	case 12:
	default:
	case 13:
		res52 = true;
	case 14:
		res53 = true;
		break; 
	}
	// default case intermixed, with break
	cond6 = 16;
	res61 = false; 
	res62 = false; 
	res63 = false;
	switch (cond6) {
	case 10:
	case 11:
		res61 = true;
	case 12:
	default:
	case 13:
		res62 = true;
		break; 
	case 14:
		res63 = true;
		break; 
	}
	// continue test
	i = 0; 
	res71 = false;
	res72 = false;
	res73 = false; 
	while (i < 6) {
		print_err(res7);
		res7[i*3] = res71;
		res7[i*3+1] = res72;
		res7[i*3+2] = res73;
	
		res71 = false;
		res72 = false;
		res73 = false;
	
		switch (i) {
		case 0:
		case 1:
			res71 = true;
			print_err('res71: ' + res71);
		case 2:
		default:
		case 3:
			res72 = true;
			print_err('res72: ' + res72);
			i++; 
			continue; 
		case 4:
			res73 = true;
			print_err('res73: ' + res73);
			break; 
		} 
		i++; 
	} 
	print_err(res7);
	// return test
	
	for (integer i=0; i<6; i++) {
		res8[i] = switchFunction(i);
	}
	return 0;
}

function string switchFunction(integer cond) { 
	string ret = ''; 
	switch (cond) {
	case 0:
		ret = ret + 0;
	case 1:
		ret = ret + 1;
	case 2:
		ret = ret + 2; 
		default:
	case 3:
		ret = ret + 3; 
		return ret; 
	case 4:
		ret = ret + 4;
		return ret; 
	} 
}