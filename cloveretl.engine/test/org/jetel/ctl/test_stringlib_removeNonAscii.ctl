string test1;
string test2;

string test_empty;
string test_null;


function integer transform() {
	test1 = removeNonAscii("Sun \u1690is shining");
	test2 = removeNonAscii("\u1695\u1687");

	test_empty = removeNonAscii('');
//	test_null = removeNonAscii(null);
	return 0;
}