integer i;
integer j;
integer itimesj;
long l;
long m;
long ltimesm;
long mtimesl;
long mtimesi;
long itimesm;
number n;
number m1;
number ntimesm1;
number ntimesj;
number jtimesn;
number m1timesm;
number mtimesm1;
decimal d;
decimal d1;
decimal dtimesd1;
decimal dtimesj;
decimal jtimesd;
decimal dtimesm;
decimal mtimesd;
decimal dtimesn;
decimal ntimesd;

function integer transform() {
	i=10;
	j=100;
	itimesj=i*j;
	print_err("times integer:"+itimesj);
	
	l=(0x7fffffffl/10l);
	print_err(l);
	
	m=0x7fffffffl;
	print_err(m);
	
	ltimesm=l*m;
	print_err("times long:"+ltimesm);
	
	mtimesl=m*l;
	print_err("times long:"+mtimesl);
	
	mtimesi=m*i;
	print_err("long times integer:"+mtimesi);
	
	itimesm=i*m;
	print_err("integer times long:"+itimesm);
	
	n=0.1;
	print_err(n);
	
	m1=0.001;
	print_err(m1);
	
	ntimesm1=n*m1;
	print_err("times number:"+ntimesm1);
	
	ntimesj=n*j;
	print_err("number times integer:"+ntimesj);
	
	jtimesn=j*n;
	print_err("integer times number:"+jtimesn);
	
	m1timesm=m1*m;
	print_err("number times long:"+m1timesm);
	
	mtimesm1=m*m1;
	print_err("long times number:"+mtimesm1);
	
	d=0.1D;
	print_err(d);
	
	d1=0.0001D;
	print_err(d1);
	
	dtimesd1=d*d1;
	print_err("times decimal:"+dtimesd1);
	
	dtimesj=d*j;
	print_err("decimal times integer:"+dtimesj);
	
	jtimesd=j*d;
	print_err("integer times decimal:"+jtimesd);
	
	dtimesm=d*m;
	print_err("decimal times long:"+dtimesm);
	
	mtimesd=m*d;
	print_err("long times decimal:"+mtimesd);
	
	dtimesn=d*n;
	print_err("decimal times number:"+dtimesn);
	
	ntimesd=n*d;
	print_err("number times decimal:"+ntimesd);
	return 0;
}