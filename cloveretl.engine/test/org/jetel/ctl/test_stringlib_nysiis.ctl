string nysiis1;
string nysiis2;
string nysiis3;
string nysiis4;

string nysiis_empty;
string nysiis_null;

function integer transform() {
	nysiis1 = NYSIIS("Cope");
	nysiis2 = NYSIIS("Kipp");
	nysiis3 = NYSIIS("1234");
	nysiis4 = NYSIIS("K2 Production");

	nysiis_empty = NYSIIS('');
	nysiis_null = NYSIIS(null);

	return 0;
}