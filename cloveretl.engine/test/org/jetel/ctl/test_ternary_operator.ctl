boolean trueValue;
boolean falseValue;
integer res1;
integer res2;
integer res3;
integer res4;
integer res5;
integer res6;
integer res7;
integer res8;
integer res9;
integer res10;
integer res11;
integer res12;
integer res13;
integer res14;
integer res15;

function integer transform() {
	trueValue = true;
	falseValue = false;
	res1 = trueValue ? 1 : 2;
	res2 = falseValue ? 1 : 2;
	// nesting in true-branch
	res3 = trueValue ?  trueValue ? 1 : 2 : 3;
	res4 = trueValue ?  falseValue ? 1 : 2 : 3; 
	res5 = falseValue ?  trueValue ? 1 : 2 : 3; 
	// nesting in false-branch
	res6 = falseValue ?  1 : trueValue ? 2 : 3; 
	res7 = falseValue ?  1 : falseValue ? 2 : 3;
	// nesting in both branches
	res8 = trueValue ?  trueValue ? 1 : 2 : trueValue ? 3 : 4;
	res9 = trueValue ?  trueValue ? 1 : 2 : falseValue ? 3 : 4;
	res10 = trueValue ?  falseValue ? 1 : 2 : trueValue ? 3 : 4;
	res11 = falseValue ?  trueValue ? 1 : 2 : trueValue ? 3 : 4;
	res12 = trueValue ?  falseValue ? 1 : 2 : falseValue ? 3 : 4;
	res13 = falseValue ?  trueValue ? 1 : 2 : falseValue ? 3 : 4;
	res14 = falseValue ?  falseValue ? 1 : 2 : trueValue ? 3 : 4;
	res15 = falseValue ?  falseValue ? 1 : 2 : falseValue ? 3 : 4;
	return 0;
}