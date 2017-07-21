byte packedLong;
byte nullLiteralOutput;
byte nullVariableOutput;

function integer transform() {
	packedLong = long2packDecimal(5000l);
	nullLiteralOutput = long2packDecimal(null);
	long nullVariable = null;
	nullVariableOutput = long2packDecimal(nullVariable);
	return 0;
}
	