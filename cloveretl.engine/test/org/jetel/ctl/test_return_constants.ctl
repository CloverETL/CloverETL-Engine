
integer skip;
integer ok;
integer all;
integer stop;

function integer stop_function(){
	return STOP;
}

function integer skip_function() {
	return SKIP;
}

function integer ok_function() {
	return OK;
}

function integer all_function() {
	return ALL;
}

function integer transform() {
	skip = skip_function();
	ok = ok_function();
	all = all_function();
	stop = stop_function();
	return SKIP;
	
}