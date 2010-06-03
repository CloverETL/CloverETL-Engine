integer i;
integer j;
integer iminusj;
long l;
long m;
long lminusm;
long mminusi;
long iminusm;
number n;
number m1;
number nminusm1;
number nminusj;
number jminusn;
number m1minusm;
number mminusm1;
decimal d;
decimal d1;
decimal dminusd1;
decimal dminusj;
decimal jminusd;
decimal dminusm;
decimal mminusd;
decimal dminusn;
decimal nminusd;

function integer transform() {
	i=10;
	j=100;
	iminusj=i-j;
	printErr("minus integer:"+iminusj);
	
	l=(0x7fffffffl/10l);
	printErr(l);
	
	m=0x7fffffffl;
	printErr(m);

	lminusm=l-m;
	printErr("minus long:" + lminusm);
	
	mminusi=m-i;
	printErr("long minus integer:" + mminusi);
	
	iminusm=i-m;
	printErr("integer minus long:"+iminusm);
	
	n=0.1;
	printErr(n);
	
	m1=0.001;
	printErr(m1);
	
	nminusm1=n-m1;
	printErr("minus number:"+nminusm1);
	
	nminusj=n-j;
	printErr("number minus integer:"+nminusj);

	jminusn=j-n;
	printErr("integer minus number:"+jminusn);
	
	m1minusm=m1-m;
	printErr("number minus long:"+m1minusm);
	
	mminusm1=m-m1;
	printErr("long minus number:"+mminusm1);
	
	d=0.1D;
	printErr(d);
	
	d1=0.0001D;
	printErr(d1);
	
	dminusd1=d-d1;
	printErr("minus decimal:"+dminusd1);
	
	dminusj=d-j;
	printErr("decimal minus integer:"+dminusj);
	
	jminusd=j-d;
	printErr("integer minus decimal:"+jminusd);
	
	dminusm=d-m;
	printErr("decimal minus long:"+dminusm);
	
	mminusd=m-d;
	printErr("long minus decimal:"+mminusd);
	
	dminusn=d-n;
	printErr("decimal minus number:"+dminusn);
	
	nminusd=n-d;
	printErr("number minus decimal:"+nminusd);
	return 0;
}