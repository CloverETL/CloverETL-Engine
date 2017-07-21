string resultNoParams;
string resultFalseFalseParams;
string resultTrueFalseParams;
string resultFalseTrueParams;
string resultTrueTrueParams;

string pattern = 'Special character representing new line is: \n calling CTL function `uppercase("message")`; $DATAIN_DIR=${DATAIN_DIR}';

function integer transform() {
	resultNoParams = resolveParams(pattern);
	resultFalseFalseParams = resolveParams(pattern, false, false);
	resultTrueFalseParams = resolveParams(pattern, true, false);
	resultFalseTrueParams = resolveParams(pattern, false, true);
	resultTrueTrueParams = resolveParams(pattern, true, true);
	
	return 0;
}