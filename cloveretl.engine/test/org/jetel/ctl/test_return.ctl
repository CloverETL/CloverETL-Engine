integer lhs;
integer rhs;
integer res;

function integer transform() {
	lhs = 1;
	rhs = 2;
	res = sum(lhs,rhs);
	return 0;
}

function integer sum(integer a, integer b) {
	return a+b;
} 
