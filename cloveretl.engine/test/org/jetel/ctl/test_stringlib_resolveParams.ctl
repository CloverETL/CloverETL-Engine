string resultNoParams;
string resultFalseParam;
string resultTrueParam;

string pattern = 'Special character representing new line is: \n calling CTL function `uppercase("message")`; $DATAIN_DIR=${DATAIN_DIR}';

function integer transform() {
	resultNoParams = resolveParams(pattern);
	resultFalseParam = resolveParams(pattern, false);
	resultTrueParam = resolveParams(pattern, true);
	
	return 0;
}