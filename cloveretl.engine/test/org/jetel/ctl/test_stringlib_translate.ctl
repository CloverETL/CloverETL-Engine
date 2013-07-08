string trans;
string trans1;
string trans2;
string trans3;
string trans4;
string trans5;

string test_empty1;
string test_empty2;
string test_null;

function integer transform() {
	trans=translate('hello','leo','pii');
	trans1=translate('hello','leo','pi');
	trans2=translate('hello','leo','piims');
	trans3=translate('hello','hleo','');
	trans4=translate('my language needs the letter e', 'egms', 'X');
	trans5=translate('hello','','abc');
	
	test_empty1 = translate('','abc','opr');
	test_empty2 = translate('','','r');
	
//	test_null = translate(null,'abc','opr');
	printErr("String: ["+test_empty2+"]");
	return 0;
}