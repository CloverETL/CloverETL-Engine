number sqrtPi;
number sqrt9;

function integer transform() {
	integer i=9;

	sqrtPi=sqrt(pi());
	sqrt9=sqrt(i);
	
	printErr('sqrtPi='+sqrtPi);
	printErr('sqrt(-1)='+sqrt(-1));
	return 0;
}