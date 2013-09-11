string input;
byte base64input;
byte nullLiteralOutput;
byte nullVariableOutput;

function integer transform() {
	input = 'The quick brown fox jumps over the lazy dog';
	base64input = base64byte(input);
	nullLiteralOutput = base64byte(null);
	string nullVariable = null;
	nullVariableOutput = base64byte(nullVariable);
	return 0;
}