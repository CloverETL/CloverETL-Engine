//issue #5125 - CTL2: "for" cycle is EXTREMELY memory consuming

integer COUNT = 10000000;
integer counter = 0;

function integer transform() {
	
	for (integer i = 0; i < COUNT; i++) {
		counter++;
	}
	
	return 0;
}