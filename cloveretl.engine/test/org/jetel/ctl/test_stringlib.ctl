string s;
integer lenght;
string subs;
string upper;
string lower;
string t;
decimal l;
string c;
date datum;
date born;
long ddiff;
date otherdate;
boolean isn;
decimal s1;
string rep;
string rep1;
decimal stdecimal;
double stdouble;
long stlong;
integer stint;
integer i;
string nts;
date newdate;
integer dtn;
integer ii;
date ndate;
string dts;
string lef;
string righ;
integer j;
integer charCount;

function integer transform() {
	s='hello world';
	lenght=5;
	subs=substring(s,1,lenght);
	printErr('original string:'+s );
	printErr('substring:'+subs );
	
	upper=upperCase(subs);
	printErr('to upper case:'+upper );
	
	lower=lowerCase(subs+'hI   ');
	printErr('to lower case:'+lower );
	
	t=trim('	  im  '+lower);
	printErr('after trim:'+t );
	
	l=length(upper);
	printErr('length of '+upper+':'+l );
	
	c=concat(lower,upper, "2,today is ", date2str(today(), "yyyy MMM dd"));
	printErr('concatenation \"'+lower+'\"+\"'+upper+'\"+2+\",today is \"+today():'+c );
	
	//born=nvl($Born,today()-365);
	//printErr('born=' + born);
	
	//datum=dateadd(born,100,millisec);
	//printErr('dataum = ' + datum );
	
	//otherdate=today();
	//ddiff=datediff(born,otherdate,year);
	//printErr('date diffrence:'+ddiff );
	//printErr('born: '+born+' otherdate: '+otherdate);
	
	isn=isnull(ddiff);
	printErr(isn );
	
	s1=nvl(l+1,1);
	printErr(s1 );
	
	rep1=replace("The dog says meow. All DOGs say meow.", "[dD][oO][gG]", "cat");
	
	rep=replace(c,'[lL]','t');
	printErr(rep1);
	
	stdecimal=str2decimal('2.5125e-1');
	stdouble=str2double('2.5125e-1');
	stlong=str2long('805421451215');
	stint=str2integer('-152456');
	printErr(stdecimal);
	printErr(stdouble);
	printErr(stlong);
	printErr(stint);
	
	i = str2integer('1234');
	
	nts=num2str(10,4);
	printErr(nts );
	
	newdate=2001-12-20 16:30:04;
	dtn=date2num(newdate,month);
	printErr('month: ' + dtn );
	
	ii=iif(newdate<2000-01-01,20,21);
	printErr('ii:'+ii);
	printStack();
	
	ndate=2002-12-24;
	dts=date2str(ndate,'yy.MM.dd');
	printErr('date to string:'+dts);
	printErr(str2date(dts,'yy.MM.dd'));
	
	lef=left(dts,5);
	righ=right(dts,5);
	printErr('s=word, soundex='+soundex('word'));
	printErr('s=world, soundex='+soundex('world'));
	
	for (j=0;j<length(s);j++) {
		printErr(charAt(s,j));
	};
	
	charCount = countChar('mimimichal','i');
	printErr(charCount);
	return 0;
}