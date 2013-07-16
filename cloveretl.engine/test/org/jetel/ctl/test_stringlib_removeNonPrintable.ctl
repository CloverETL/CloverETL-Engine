string input;
string nonPrintableRemoved;

string test_empty;
string test_null;
function integer transform() {
	input="A\u000b\u000bH\u000b\u000bO\u000b\u000bJ";
	nonPrintableRemoved = removeNonPrintable(input);
	
	test_empty = removeNonPrintable('');
	test_null = removeNonPrintable(null);
	return 0;
}