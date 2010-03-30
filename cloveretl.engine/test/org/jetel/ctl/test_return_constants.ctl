
int skip;
int ok;
int all;

function int skip_function() {
	return SKIP;
}

function int ok_function() {
	return OK;
}

function int all_function() {
	return ALL;
}

function int transform() {
	skip = skip_function();
	ok = ok_function();
	all = all_function();
	
	return SKIP;
	
}