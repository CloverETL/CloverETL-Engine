double[] ret;

function integer transform(){
	ret[0] = tan(pi()/2);
	ret[1] = tan(2*pi());
	ret[2] = tan(pi());
	ret[3] = tan(0.0);
	ret[4] = tan(12.44d);
	ret[5] = tan(78);
	ret[6] = tan(725l);
	number e1;
	number e2;
	ret[7] = tan(e1);
	ret[8] = tan(e2);
	printErr(ret);
	return 0;
}