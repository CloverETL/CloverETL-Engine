string soundex1;
string soundex2;

string test_empty;
string test_null;

function integer transform() {
	soundex1 = soundex('word');
	soundex2 = soundex('world');
	test_empty = soundex('');
	test_null = soundex(null);
	return 0;
}