string concat;
string concat1;
string concat2;
string concat3;

string test_null;
string test_null2;
string test_null3;

function integer transform() {
	concat=concat("");
	concat1=concat("ello hi   ", 'ELLO ', "2,today is ", date2str(today(), "yyyy MMM dd"));
	concat2=concat("","");
	concat3=concat("","","clover");
//	test_null=concat(null,"");
//	test_null2=concat("",null);
//	test_null3=concat("sky",null,"is",'',null,"blue");
//	printErr('concatenation:' + test_null);
	return 0;
}