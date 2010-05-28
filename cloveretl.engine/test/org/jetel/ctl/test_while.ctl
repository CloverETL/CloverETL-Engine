integer[] res1;
integer ctr1;
integer[] res2;
integer ctr2;
integer[] res3;
integer ctr3;
decimal d;

function integer transform() {
	printErr('while1: simple loop'); 
	ctr1 = -1;
	while (++ctr1<3) {
		res1= res1 + ctr1;
		printErr('Iteration ' + ctr1);
	} 
	// continue test
	printErr('while2: continue loop'); 
	ctr2 = -1;
	while (++ctr2<3) {
		if (ctr2 == 1) {
			continue;
		} 	
		res2 = res2 + ctr2;
		printErr('Iteration ' + ctr2);
	}
	// break test
	printErr('while3: break loop'); 
	ctr3 = -1;
	while (++ctr3<3) {
		if (ctr3 == 1) {
			break;
		} 	
		res3= res3 + ctr3;
		printErr('Iteration ' + ctr3);
	}
	d = 10;
	while ((d=d+1)<20) {
		printErr(d);
	}
	return 0;
}