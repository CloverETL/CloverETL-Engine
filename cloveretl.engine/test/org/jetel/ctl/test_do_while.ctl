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
	print_err('do-while1: simple loop'); 
	ctr1 = 0;
	do {
		res1[ctr1]=ctr1;
		print_err('Iteration ' + ctr1);
	} while (++ctr1<3) 
	print_err(res1); 
	// continue test
	print_err('do-while2: continue loop'); 
	ctr2 = 0;
	do {
		if (ctr2 == 1) {
			continue;
		} 	
		res2[ctr2]=ctr2;
		print_err('Iteration ' + ctr2);
	} while (++ctr2<3)
	print_err(res2); 
	// break test
	print_err('do-while3: break loop'); 
	ctr3 = 0;
	do {
		if (ctr3 == 1) {
			break;
		} 	
		res3[ctr3]=ctr3;
		print_err('Iteration ' + ctr3);
	} while (++ctr3<3)
	print_err(res3);
	// decimal test
	print_err('do-while4: decimal loop'); 
	ctr4 = 0;
	i4 = 0;
	do {
		if (ctr4 == 1) {
			break;
		} 	
		res4[i4]=ctr4;
		print_err('Iteration ' + ctr4);
	} while (++ctr4<3)
	print_err(res4);
	return 0;
}