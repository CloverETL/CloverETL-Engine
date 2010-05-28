integer[] res1;
integer[] res2;
integer[] res3;
integer[] res4;
integer ctr4; 
integer[] res5;
integer[] res6;
integer[] res7;
integer ctr7; 
integer[] res8;
decimal[] res9;
integer i9; 

function integer transform() {
	printErr('for4: simple loop'); 
	for (integer ctr1=0; ctr1 < 3; ctr1++) {
		res1[ctr1]=ctr1; 
		printErr('Iteration ' + ctr1);
	} 
	// continue test
	printErr('for2: continue loop'); 
	for (integer ctr2=0; ctr2<3; ctr2++) {
		if (ctr2 == 1) {
			continue;
		} 	
		res2[ctr2]=ctr2; 
		printErr('Iteration ' + ctr2);
	}
	// break test
	printErr('for3: break loop'); 
	for (integer ctr3=0; ctr3<3; ctr3++) {
		if (ctr3 == 1) {
			break;
		} 	
		res3[ctr3]=ctr3; 
		printErr('Iteration ' + ctr3);
	}
	// empty init
	printErr('for4: empty init'); 
	ctr4 = 0; 
	for (; ctr4 < 3; ctr4++) {
		res4[ctr4]=ctr4; 
		printErr('Iteration ' + ctr4);
	}
	// empty update
	printErr('for5: empty update'); 
	for (integer ctr5=0; ctr5 < 3;) {
		res5[ctr5]=ctr5; 
		printErr('Iteration ' + ctr5);
		ctr5++;
	}
	// empty final condition
	printErr('for6: empty final condition'); 
	for (integer ctr6=0; ; ctr6++) {
		if (ctr6 >= 3) {
			break;
		}
		res6[ctr6]=ctr6; 
		printErr('Iteration ' + ctr6);
	}
	// all conditions empty
	printErr('for7: all conditions empty'); 
	ctr7=0; 
	for (;;) {
		if (ctr7 >= 3) {
			break;
		}
		res7[ctr7]=ctr7; 
		printErr('Iteration ' + ctr7);
		ctr7++; 
	}
	// nested loop
	printErr('for8: nested loop'); 
	for (integer ctr8=0; ctr8<3; ctr8++) {
		for (integer ctr81=0; ctr81 < 5; ctr81++) {
			if (ctr81 == 1) {
				continue;
			} 	
			res8[ctr81]=ctr81; 
			printErr('Iteration ' + ctr8 + ' ' + ctr81);
		}	
	}
	// loop with decimal condition
	printErr('for9: decimal loop'); 
	i9 = 0; 
	for (decimal ctr9=0; ctr9++<3;) {
		res9[i9++]=ctr9; 
		printErr('Iteration ' + ctr9 + ' ' + i9);
	}
	return 0;
}