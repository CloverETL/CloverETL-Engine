
integer result;

function integer test_1() {
	raiseError("blbecku");
	//return 1;
}

function integer test_2() {
	return str2integer("sadfsdf");
	//return 2;
}

function integer test_3() {
	return 3;
}


function integer transform() {

	result = test_1() : test_2() : test_3() : 0;
	printErr(result);
		
	return 0;
}