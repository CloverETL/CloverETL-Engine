string bitsAsString1;
string bitsAsString2;
string bitsAsString3;
string nullLiteralOutput;
string nullVariableOutput;

function integer transform() {
 	bitsAsString1 = bits2str(str2bits('0'));
 	bitsAsString2 = bits2str(str2bits('11111111'));
 	bitsAsString3 = bits2str(str2bits('0101000001001101101'));
 	nullLiteralOutput = bits2str(null);
 	byte nullVariable = null;
 	nullVariableOutput = bits2str(nullVariable);
 	return 0;
}