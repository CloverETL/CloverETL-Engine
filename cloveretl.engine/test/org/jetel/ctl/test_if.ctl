boolean cond1;
boolean res1;
boolean cond2;
boolean res21;
boolean res22;
boolean cond3;
boolean res31;
boolean res32;
boolean cond4;
boolean res41;
boolean res42;
boolean res43;
boolean cond5;
boolean res51;
boolean res52;
boolean res53;
boolean res54;
boolean cond61;
boolean cond62;
boolean res61;
boolean res62;
boolean cond71;
boolean cond72;
boolean res71;
boolean res72;
boolean res73;
boolean cond81;
boolean cond82;
boolean res81;
boolean res82;
boolean res83;
boolean cond9;
boolean res91;
boolean res92;
boolean cond10;
boolean res101;
boolean res102;
boolean res103;
boolean res104;
integer i;
integer j;
boolean res11;

function integer transform() {
	printErr('case1: simple if');
	cond1 = true;
	res1 = false;
	if (cond1)
		res1 = true;
	// if with mutliple statements (block)
	cond2 = true;
	res21 = false;
	res22 = false;
	printErr('case2: if with block ');
	if (cond2) {
		printErr('cond2: inside block');
		res21 = true;
		res22 = true;
	}
	// else with single statement
	cond3 = false;
	res31 = false;
	res32 = false;
	printErr('case3: simple-if and simple-else');
	if (cond3) 
		res31 = true;
	else 
		res32 = true;
	// else with multiple statements (block)
	printErr('case4: simple-if, block-else');
	cond4 = false;
	res41 = false;
	res42 = false;
	res43 = false;
	if (cond4) 
		res41 = true;
	else {
	printErr('cond4: within else body');
		res42 = true;
		res43 = true;
	}
	// if with block, else with block
	printErr('case5: block-if, block-else');
	cond5 = false;
	res51 = false;
	res52 = false;
	res53 = false;
	res54 = false;
	if (cond5) {
		res51 = true;
		res52 = true;
	} else {
		printErr('cond5: within else body');
		res53 = true;
		res54 = true;
	}
	// else-if with single statement
	printErr('case6: simple if, simple else-if');
	cond61 = false;
	cond62 = true;
	res61 = false;
	res62 = false;
	if (cond61) 
		res61 = true;
	 else if (cond62)
		res62 = true;
	// else-if with multiple statements
	printErr('case7: simple if, block else-if');
	cond71 = false;
	cond72 = true;
	res71 = false;
	res72 = false;
	res73 = false;
	if (cond71) 
		res71 = true;
	 else if (cond72) {
		printErr('cond72: within else-if body');
		res72 = true;
		res73 = true;
	}
	// if-elseif-else test
	printErr('case8: if-else/if-else ');
	cond81 = false;
	cond82 = false;
	res81 = false;
	res82 = false;
	res83 = false;
	if (cond81) {
		res81 = true;
	} else if (cond82) {
		res82 = true;
	} else {
		res83 = true;
	}
	printErr('case9: if with inactive else');
	// if with single statement + inactive else
	cond9 = true;
	res91 = false;
	res92 = false;
	if (cond9) 
		res91 = true;
	else 
		res92 = true;
	// if with multiple statements + inactive else
	// if with block, else with block
	printErr('case10: if-block with inactive else-block');
	cond10 = true;
	res101 = false;
	res102 = false;
	res103 = false;
	res104 = false;
	if (cond10) {
		res101 = true;
		res102 = true;
	} else {
		res103 = true;
		res104 = true;
	}
	// if with simple condition
	printErr('case 11: if with expression condition');
	i=0;
	j=1;
	res11 = false;
	if (i < j) {
		printErr('i<j');
		res11 = true;
	}
	return 0;
}