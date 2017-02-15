string pattern = 'Special character representing new line is: \n calling CTL function `uppercase("message")`; $DATAIN_DIR=${DATAIN_DIR}';

function integer transform() {
	resolveParams(pattern, false, true);
	
	return 0;
}