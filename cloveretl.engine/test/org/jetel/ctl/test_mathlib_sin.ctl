number[] ret;

function integer transform(){
	ret[0] = sin(1.0);
	ret[1] = sin(25.9d);
	ret[2] = sin(256L);
	ret[3] = sin(123);
	double e1;
	decimal e2;
	ret[4] = sin(e1);
	ret[5] = sin(e2);
	return 0;
}