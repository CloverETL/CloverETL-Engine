string s1;
string s2;
string s3;
string s4;
string s5;
string s6;
string s7;

function integer transform() {
	s1=chop("hello\n");
	s6=chop("hello\r");
	s5=chop("hello\n\n");
	s2=chop("hello\r\n");
	s7=chop("hello\nworld\r\n");
	s3=chop("hello world",'world');
	s4=chop("hello world",' world');
	return 0;
}
