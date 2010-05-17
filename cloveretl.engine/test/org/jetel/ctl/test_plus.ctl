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
	print_err("plus integer:" + iplusj);
	
	l=(0x7fffffffl/10l);
	print_err(l);
	
	m=0x7fffffffl;
	print_err(m);

	lplusm=l+m;
	print_err("plus long:" + lplusm);
	
	mplusl=m+l;
	print_err("plus long:" + mplusl);
	
	mplusi=m+i;
	print_err("long plus integer:" + mplusi);
	
	iplusm=i+m;
	print_err("integer plus long:" + iplusm);
	
	n=0.1;
	print_err(n);
	
	m1=0.001;
	print_err(m1);
	
	nplusm1=n+m1;
	print_err("plus number:" + nplusm1);
	
	nplusj=n+j;
	print_err("number plus integer:" + nplusj);
	
	jplusn=j+n;
	print_err("integer plus number:" + jplusn);

	m1plusm=m1+m;
	print_err("number plus long:" + m1plusm);

	mplusm1=m+m1;
	print_err("long plus number:"+mplusm1);
	
	d=0.1D;
	print_err(d);

	d1=0.0001D;
	print_err(d1);
	
	dplusd1=d+d1;
	print_err("plus decimal:" + dplusd1);
	
	dplusj=d+j;
	print_err("decimal plus integer:" + dplusj);
	
	jplusd=j+d;
	print_err("integer plus decimal:" + jplusd);
	
	dplusm=d+m;
	print_err("decimal plus long:" + dplusm);
	
	mplusd=m+d;
	print_err("long plus decimal:"+mplusd);
	
	dplusn=d+n;
	print_err("decimal plus number:" + dplusn);
	
	nplusd=n+d;
	print_err("number plus decimal:"+nplusd);
	
	s="hello";
	print_err(s);
	
	s1=" world";
	print_err(s1);
	
	spluss1=s+s1;
	print_err("adding strings:" + spluss1);
	
	splusj=s+j;
	print_err("string plus integer:" + splusj);
	
	jpluss=j+s;
	print_err("integer plus string:" + jpluss);
	
	splusm=s+m;
	print_err("string plus long:" + splusm);
	
	mpluss=m+s;
	print_err("long plus string:" + mpluss);
	
	splusm1=s+m1;
	print_err("string plus number:" + splusm1);
	
	m1pluss=m1+s;
	print_err("number plus string:" + m1pluss);
	
	splusd1=s+d1;
	print_err("string plus decimal:" + splusd1);
	
	d1pluss=d1+s;
	print_err("decimal plus string:" + d1pluss);
	return 0;
}