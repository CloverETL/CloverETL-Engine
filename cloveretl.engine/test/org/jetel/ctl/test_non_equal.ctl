integer i;
integer j;
boolean inei;
boolean inej;
boolean jnei;
boolean jnej;
long l;
boolean lnei;
boolean inel;
boolean lnej;
boolean jnel;
boolean lnel;
decimal d;
boolean dnei;
boolean ined;
boolean dnej;
boolean jned;
boolean dnel;
boolean lned;
boolean dned;

function integer transform() {
	i=10;
	print_err("i="+i);
	
	j=9;
	print_err("j="+j);
	
	inei=(i!=i);
	print_err("inei=" + inei);
	
	inej=(i!=j);
	print_err("inej=" + inej);
	
	jnei=(j!=i);
	print_err("jnei=" + jnei);
	
	jnej=(j!=j);
	print_err("jnej=" + jnej);
	
	l=10;
	print_err("l="+l);
	
	lnei=(l<>i);
	print_err("lnei=" + lnei);
	
	inel=(i<>l);
	print_err("inel=" + inel);
	
	lnej=(l<>j);
	print_err("lnej=" + lnej);
	
	jnel=(j<>l);
	print_err("jnel=" + jnel);
	
	lnel=(l<>l);
	print_err("lnel=" + lnel);
	
	d=10;
	print_err("d="+d);
	
	dnei=d.ne.i;
	print_err("dnei=" + dnei);
	
	ined=i.ne.d;
	print_err("ined=" + ined);
	
	dnej=d.ne.j;
	print_err("dnej=" + dnej);
	
	jned=j.ne.d;
	print_err("jned=" + jned);
	
	dnel=d.ne.l;
	print_err("dnel=" + dnel);
	
	lned=l.ne.d;
	print_err("lned=" + lned);
	
	dned=d.ne.d;
	print_err("dned=" + dned);
	
	return 0;
}