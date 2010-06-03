integer i;
integer j;
integer idividej;
long l;
long m;
long ldividem;
long mdividei;
long idividem;
number n;
number m1;
number ndividem1;
number ndividej;
number jdividen;
number m1dividem;
number mdividem1;
decimal d;
decimal d1;
decimal ddivided1;
decimal ddividej;
decimal jdivided;
decimal ddividem;
decimal mdivided;
decimal ddividen;
decimal ndivided;

function integer transform() {
	i=10;
	j=100;
	idividej=i/j;
	printErr("divide integer:"+idividej);
	
	l=(0x7fffffffl/10l);
	printErr(l);
	
	m=0x7fffffffl;
	printErr(m);
	
	ldividem=l/m;
	printErr("divide long:"+ldividem);
	
	mdividei=m/i;
	printErr("long divide integer:"+mdividei);
	
	idividem=i/m;
	printErr("integer divide long:"+idividem);
	
	n=0.1;
	printErr(n);
	
	m1=0.001;
	printErr(m1);
	
	ndividem1=n/m1;
	printErr("divide number:"+ndividem1);
	
	ndividej=n/j;
	printErr("number divide integer:"+ndividej);
	
	jdividen=j/n;
	printErr("integer divide number:"+jdividen);
	
	m1dividem=m1/m;
	printErr("number divide long:"+m1dividem);
	
	mdividem1=m/m1;
	printErr("long divide number:"+mdividem1);
	
	d=0.1D;
	printErr(d);
	
	d1=0.0001D;
	printErr(d1);
	
	ddivided1=d/d1;
	printErr("divide decimal:"+ddivided1);
	
	ddividej=d/j;
	printErr("decimal divide integer:"+ddividej);
	
	jdivided=j/d;
	printErr("integer divide decimal:"+jdivided);
	
	ddividem=d/m;
	printErr("decimal divide long:"+ddividem);
	
	mdivided=m/d;
	printErr("long divide decimal:"+mdivided);
	
	ddividen=d/n;
	printErr("decimal divide number:"+ddividen);
	
	ndivided=n/d;
	printErr("number divide decimal:"+ndivided);
	
	return 0;
}