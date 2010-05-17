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
	print_err("minus integer:"+iminusj);
	
	l=(0x7fffffffl/10l);
	print_err(l);
	
	m=0x7fffffffl;
	print_err(m);

	lminusm=l-m;
	print_err("minus long:" + lminusm);
	
	mminusi=m-i;
	print_err("long minus integer:" + mminusi);
	
	iminusm=i-m;
	print_err("integer minus long:"+iminusm);
	
	n=0.1;
	print_err(n);
	
	m1=0.001;
	print_err(m1);
	
	nminusm1=n-m1;
	print_err("minus number:"+nminusm1);
	
	nminusj=n-j;
	print_err("number minus integer:"+nminusj);

	jminusn=j-n;
	print_err("integer minus number:"+jminusn);
	
	m1minusm=m1-m;
	print_err("number minus long:"+m1minusm);
	
	mminusm1=m-m1;
	print_err("long minus number:"+mminusm1);
	
	d=0.1D;
	print_err(d);
	
	d1=0.0001D;
	print_err(d1);
	
	dminusd1=d-d1;
	print_err("minus decimal:"+dminusd1);
	
	dminusj=d-j;
	print_err("decimal minus integer:"+dminusj);
	
	jminusd=j-d;
	print_err("integer minus decimal:"+jminusd);
	
	dminusm=d-m;
	print_err("decimal minus long:"+dminusm);
	
	mminusd=m-d;
	print_err("long minus decimal:"+mminusd);
	
	dminusn=d-n;
	print_err("decimal minus number:"+dminusn);
	
	nminusd=n-d;
	print_err("number minus decimal:"+nminusd);
	return 0;
}