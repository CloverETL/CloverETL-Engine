string[] alphaResult;
string[] bravoResult;
integer[] countResult;
string[] charlieResult;
integer idx;

function integer transform() {
	idx = 0;
	for (integer i=0; i<2; i++) {
		alphaResult[i] = lookup(TestLookup).get('Alpha',1).City; 
		bravoResult[i] = lookup(TestLookup).get('Bravo',2).City;
		countResult[i] = lookup(TestLookup).count('Charlie',3);
		for (integer count=0; count<countResult[i]; count++) {
			charlieResult[idx++] = lookup(TestLookup).next().City;
		}
		print_err('Freeing lookup table');
		lookup(TestLookup).free();
		print_err('Initializing lookup table');
		lookup(TestLookup).init();
	}
	return 0;
}