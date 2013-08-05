string input;
byte fromHex;
byte test_null;

function integer transform() {
	input = '41626563656461207a65646c612064656461';
	fromHex = hex2byte(input);
	test_null = hex2byte(null);
	return 0;
}