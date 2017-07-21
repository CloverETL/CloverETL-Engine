integer i;
integer j;
integer iplusj;
long l;
long m;
long lplusm;
long mplusl;
long mplusi;
long iplusm;
number n;
number m1;
number nplusm1;
number nplusj;
number jplusn;
number m1plusm;
number mplusm1;
decimal d;
decimal d1;
decimal dplusd1;
decimal dplusj;
decimal jplusd;
decimal dplusm;
decimal mplusd;
decimal dplusn;
decimal nplusd;
string s;
string s1;
string spluss1;
string splusj;
string jpluss;
string splusm;
string mpluss;
string splusm1;
string m1pluss;
string splusd1;
string d1pluss;

function integer transform() {
	i=10;
	j=100;
	iplusj=i+j;
	printErr("plus integer:" + iplusj);
	
	l=(0x7fffffffl/10l);
	printErr(l);
	
	m=0x7fffffffl;
	printErr(m);

	lplusm=l+m;
	printErr("plus long:" + lplusm);
	
	mplusl=m+l;
	printErr("plus long:" + mplusl);
	
	mplusi=m+i;
	printErr("long plus integer:" + mplusi);
	
	iplusm=i+m;
	printErr("integer plus long:" + iplusm);
	
	n=0.1;
	printErr(n);
	
	m1=0.001;
	printErr(m1);
	
	nplusm1=n+m1;
	printErr("plus number:" + nplusm1);
	
	nplusj=n+j;
	printErr("number plus integer:" + nplusj);
	
	jplusn=j+n;
	printErr("integer plus number:" + jplusn);

	m1plusm=m1+m;
	printErr("number plus long:" + m1plusm);

	mplusm1=m+m1;
	printErr("long plus number:"+mplusm1);
	
	d=0.1D;
	printErr(d);

	d1=0.0001D;
	printErr(d1);
	
	dplusd1=d+d1;
	printErr("plus decimal:" + dplusd1);
	
	dplusj=d+j;
	printErr("decimal plus integer:" + dplusj);
	
	jplusd=j+d;
	printErr("integer plus decimal:" + jplusd);
	
	dplusm=d+m;
	printErr("decimal plus long:" + dplusm);
	
	mplusd=m+d;
	printErr("long plus decimal:"+mplusd);
	
	dplusn=d+n;
	printErr("decimal plus number:" + dplusn);
	
	nplusd=n+d;
	printErr("number plus decimal:"+nplusd);
	
	s="hello";
	printErr(s);
	
	s1=" world";
	printErr(s1);
	
	spluss1=s+s1;
	printErr("adding strings:" + spluss1);
	
	splusj=s+j;
	printErr("string plus integer:" + splusj);
	
	jpluss=j+s;
	printErr("integer plus string:" + jpluss);
	
	splusm=s+m;
	printErr("string plus long:" + splusm);
	
	mpluss=m+s;
	printErr("long plus string:" + mpluss);
	
	splusm1=s+m1;
	printErr("string plus number:" + splusm1);
	
	m1pluss=m1+s;
	printErr("number plus string:" + m1pluss);
	
	splusd1=s+d1;
	printErr("string plus decimal:" + splusd1);
	
	d1pluss=d1+s;
	printErr("decimal plus string:" + d1pluss);
	return 0;
}