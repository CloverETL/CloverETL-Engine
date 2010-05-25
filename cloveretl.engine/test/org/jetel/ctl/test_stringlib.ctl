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
	print_err('original string:'+s );
	print_err('substring:'+subs );
	
	upper=uppercase(subs);
	print_err('to upper case:'+upper );
	
	lower=lowercase(subs+'hI   ');
	print_err('to lower case:'+lower );
	
	t=trim('	  im  '+lower);
	print_err('after trim:'+t );
	
	l=length(upper);
	print_err('length of '+upper+':'+l );
	
	c=concat(lower,upper, "2,today is ", date2str(today(), "yyyy MMM dd"));
	print_err('concatenation \"'+lower+'\"+\"'+upper+'\"+2+\",today is \"+today():'+c );
	
	//born=nvl($Born,today()-365);
	//print_err('born=' + born);
	
	//datum=dateadd(born,100,millisec);
	//print_err('dataum = ' + datum );
	
	//otherdate=today();
	//ddiff=datediff(born,otherdate,year);
	//print_err('date diffrence:'+ddiff );
	//print_err('born: '+born+' otherdate: '+otherdate);
	
	isn=isnull(ddiff);
	print_err(isn );
	
	s1=nvl(l+1,1);
	print_err(s1 );
	
	rep1=replace("The dog says meow. All DOGs say meow.", "[dD][oO][gG]", "cat");
	
	rep=replace(c,'[lL]','t');
	print_err(rep1);
	
	stdecimal=str2decimal('2.5125e-1');
	stdouble=str2double('2.5125e-1');
	stlong=str2long('805421451215');
	stint=str2int('-152456');
	print_err(stdecimal);
	print_err(stdouble);
	print_err(stlong);
	print_err(stint);
	
	i = str2int('1234');
	
	nts=num2str(10,4);
	print_err(nts );
	
	newdate=2001-12-20 16:30:04;
	dtn=date2num(newdate,month);
	print_err('month: ' + dtn );
	
	ii=iif(newdate<2000-01-01,20,21);
	print_err('ii:'+ii);
	print_stack();
	
	ndate=2002-12-24;
	dts=date2str(ndate,'yy.MM.dd');
	print_err('date to string:'+dts);
	print_err(str2date(dts,'yy.MM.dd'));
	
	lef=left(dts,5);
	righ=right(dts,5);
	print_err('s=word, soundex='+soundex('word'));
	print_err('s=world, soundex='+soundex('world'));
	
	for (j=0;j<length(s);j++) {
		print_err(char_at(s,j));
	};
	
	charCount = count_char('mimimichal','i');
	print_err(charCount);
	return 0;
}