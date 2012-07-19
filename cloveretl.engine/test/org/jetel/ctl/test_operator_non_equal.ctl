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
boolean dned_different_scale;

function integer transform() {
	i=10;
	printErr("i="+i);
	
	j=9;
	printErr("j="+j);
	
	inei=(i!=i);
	printErr("inei=" + inei);
	
	inej=(i!=j);
	printErr("inej=" + inej);
	
	jnei=(j!=i);
	printErr("jnei=" + jnei);
	
	jnej=(j!=j);
	printErr("jnej=" + jnej);
	
	l=10;
	printErr("l="+l);
	
	lnei=(l<>i);
	printErr("lnei=" + lnei);
	
	inel=(i<>l);
	printErr("inel=" + inel);
	
	lnej=(l<>j);
	printErr("lnej=" + lnej);
	
	jnel=(j<>l);
	printErr("jnel=" + jnel);
	
	lnel=(l<>l);
	printErr("lnel=" + lnel);
	
	d=10;
	printErr("d="+d);
	
	dnei=d.ne.i;
	printErr("dnei=" + dnei);
	
	ined=i.ne.d;
	printErr("ined=" + ined);
	
	dnej=d.ne.j;
	printErr("dnej=" + dnej);
	
	jned=j.ne.d;
	printErr("jned=" + jned);
	
	dnel=d.ne.l;
	printErr("dnel=" + dnel);
	
	lned=l.ne.d;
	printErr("lned=" + lned);
	
	dned=d.ne.d;
	printErr("dned=" + dned);
	
	dned_different_scale = (0.0D != 0.00D);
	printErr("dned_different_scale=" + dned_different_scale);
	
	return 0;
}