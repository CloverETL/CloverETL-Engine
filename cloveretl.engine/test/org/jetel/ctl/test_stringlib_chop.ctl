string s1;
string s2;
string s3;
string s4;
string s5;
string s6;
string s7;
string s8;
string s9;
string s10;
string s11;
string s12;
string s13;
string s14;
string s15;
string s16;
string s17;
string s18;
string s19;
string s20;

function integer transform() {
	s1=chop("hello\n");
	s6=chop("hello\r");
	s5=chop("hello\n\n");
	s2=chop("hello\r\n");
	s7=chop("hello\nworld\r\n");
	s3=chop("hello world",'world');
	s4=chop("hello world",' world');
	
	s8=chop("\nhello");
	s9=chop("\rworld");
	s10=chop("\n\nhello");
	s11=chop("\r\nworld");
	
	s12=chop("mark.twain@javlin.eu", '@[a-z]*.[a-z]*$');
	s13=chop("two words", ' spider');
	
	s14=chop("\n");
	s15=chop("\r");
	s16=chop("\n\n");
	s17=chop("\r\n");
	
	s18=chop("",'aaa');
	s19=chop("word",'');
	s20=chop("",'');
	return 0;
}
