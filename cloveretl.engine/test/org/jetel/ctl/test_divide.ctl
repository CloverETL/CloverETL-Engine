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
	print_err("divide integer:"+idividej);
	
	l=(0x7fffffffl/10l);
	print_err(l);
	
	m=0x7fffffffl;
	print_err(m);
	
	ldividem=l/m;
	print_err("divide long:"+ldividem);
	
	mdividei=m/i;
	print_err("long divide integer:"+mdividei);
	
	idividem=i/m;
	print_err("integer divide long:"+idividem);
	
	n=0.1;
	print_err(n);
	
	m1=0.001;
	print_err(m1);
	
	ndividem1=n/m1;
	print_err("divide number:"+ndividem1);
	
	ndividej=n/j;
	print_err("number divide integer:"+ndividej);
	
	jdividen=j/n;
	print_err("integer divide number:"+jdividen);
	
	m1dividem=m1/m;
	print_err("number divide long:"+m1dividem);
	
	mdividem1=m/m1;
	print_err("long divide number:"+mdividem1);
	
	d=0.1D;
	print_err(d);
	
	d1=0.0001D;
	print_err(d1);
	
	ddivided1=d/d1;
	print_err("divide decimal:"+ddivided1);
	
	ddividej=d/j;
	print_err("decimal divide integer:"+ddividej);
	
	jdivided=j/d;
	print_err("integer divide decimal:"+jdivided);
	
	ddividem=d/m;
	print_err("decimal divide long:"+ddividem);
	
	mdivided=m/d;
	print_err("long divide decimal:"+mdivided);
	
	ddividen=d/n;
	print_err("decimal divide number:"+ddividen);
	
	ndivided=n/d;
	print_err("number divide decimal:"+ndivided);
	
	return 0;
}