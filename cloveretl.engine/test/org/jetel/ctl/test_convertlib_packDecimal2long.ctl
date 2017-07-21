long unpackedLong;
long nullLiteralOutput;
long nullVariableOutput;

function integer transform() {
	unpackedLong = packDecimal2long($firstInput.ByteArray);
	nullLiteralOutput = packDecimal2long(null);
	byte nullVariable = null;
	nullVariableOutput = packDecimal2long(nullVariable);
	return 0;
}