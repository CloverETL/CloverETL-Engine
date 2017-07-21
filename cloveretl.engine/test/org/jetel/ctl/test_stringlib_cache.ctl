string[] find1;
string[] find2;
string[] find3;

string rep1;
string rep2;
string rep3;

string[] split1;
string[] split2;
string[] split3;

string chop01;
string chop02;
string chop03;
string chop11;
string chop12;

function integer transform() {

	string f1 = "toto je testovaci string tro pre vyhladavanie to regexpov"; 
	string f2 = "t.?o"; 

	for (integer i = 0; i < 3; i++) {	
		find1= find("toto je testovaci string tro pre vyhladavanie to regexpov", "t.?o");
		find2= find(f1, "t.?o");
		find3= find(f1, f2);
	}

	string r1 = "The dog says meow. All DOGs say meow.";
	string r2 = "[dD][oO][gG]";
	string r3 = "cat";
	
	for (integer i = 0; i < 3; i++) {	
		rep1=replace("The dog says meow. All DOGs say meow.", "[dD][oO][gG]", "cat");
		rep2=replace(r1, "[dD][oO][gG]", r3);
		rep3=replace(r1, r2, r3);
	}
	
	
    string s1 = "one:two:three:four:five";
    string s2 = ":";
	
	for (integer i = 0; i< 3; i++) {
		split1=split("one:two:three:four:five", ":");
		split2=split(s1, ":");
		split3=split(s1, s2);
	}
	
	string c01 = "testing somesting chopesting function";
	string c02 = "est";
	
	for (integer i = 0; i< 3; i++) {
		chop01=chop("testing somesting chopesting function", "est");
		chop02=chop(c01, "est");
		chop03=chop(c01, c02);
	}
	
	string c11 = "testing end\n of lines \n\rcutting\r";
	for (integer i = 0; i< 3; i++) {
		chop11=chop("testing end\n of lines \n\rcutting\r");
		chop12=chop(c11);
	}
	
	return OK;
	
}
