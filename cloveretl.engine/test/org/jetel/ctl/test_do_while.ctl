integer[] res1;
integer ctr1;
integer[] res2;
integer ctr2;
integer[] res3;
integer ctr3;
decimal[] res4;
decimal ctr4;
integer i4;

function integer transform() {
	printErr('do-while1: simple loop'); 
	ctr1 = 0;
	do {
		res1[ctr1]=ctr1;
		printErr('Iteration ' + ctr1);
	} while (++ctr1<3) 
	printErr(res1); 
	// continue test
	printErr('do-while2: continue loop'); 
	ctr2 = 0;
	do {
		if (ctr2 == 1) {
			continue;
		} 	
		res2[ctr2]=ctr2;
		printErr('Iteration ' + ctr2);
	} while (++ctr2<3)
	printErr(res2); 
	// break test
	printErr('do-while3: break loop'); 
	ctr3 = 0;
	do {
		if (ctr3 == 1) {
			break;
		} 	
		res3[ctr3]=ctr3;
		printErr('Iteration ' + ctr3);
	} while (++ctr3<3)
	printErr(res3);
	// decimal test
	printErr('do-while4: decimal loop'); 
	ctr4 = 0;
	i4 = 0;
	do {
		if (ctr4 == 1) {
			break;
		} 	
		res4[i4]=ctr4;
		printErr('Iteration ' + ctr4);
	} while (++ctr4<3)
	printErr(res4);
	return 0;
}