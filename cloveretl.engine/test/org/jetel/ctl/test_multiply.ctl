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
	printErr("times integer:"+itimesj);
	
	l=(0x7fffffffl/10l);
	printErr(l);
	
	m=0x7fffffffl;
	printErr(m);
	
	ltimesm=l*m;
	printErr("times long:"+ltimesm);
	
	mtimesl=m*l;
	printErr("times long:"+mtimesl);
	
	mtimesi=m*i;
	printErr("long times integer:"+mtimesi);
	
	itimesm=i*m;
	printErr("integer times long:"+itimesm);
	
	n=0.1;
	printErr(n);
	
	m1=0.001;
	printErr(m1);
	
	ntimesm1=n*m1;
	printErr("times number:"+ntimesm1);
	
	ntimesj=n*j;
	printErr("number times integer:"+ntimesj);
	
	jtimesn=j*n;
	printErr("integer times number:"+jtimesn);
	
	m1timesm=m1*m;
	printErr("number times long:"+m1timesm);
	
	mtimesm1=m*m1;
	printErr("long times number:"+mtimesm1);
	
	d=0.1D;
	printErr(d);
	
	d1=0.0001D;
	printErr(d1);
	
	dtimesd1=d*d1;
	printErr("times decimal:"+dtimesd1);
	
	dtimesj=d*j;
	printErr("decimal times integer:"+dtimesj);
	
	jtimesd=j*d;
	printErr("integer times decimal:"+jtimesd);
	
	dtimesm=d*m;
	printErr("decimal times long:"+dtimesm);
	
	mtimesd=m*d;
	printErr("long times decimal:"+mtimesd);
	
	dtimesn=d*n;
	printErr("decimal times number:"+dtimesn);
	
	ntimesd=n*d;
	printErr("number times decimal:"+ntimesd);
	return 0;
}