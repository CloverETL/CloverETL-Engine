string input;
string nonPrintableRemoved;

function integer transform() {
	input="A\u000b\u000bH\u000b\u000bO\u000b\u000bJ";
	nonPrintableRemoved = removeNonPrintable(input);
	return 0;
}