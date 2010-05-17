integer i;
integer j;
boolean eq1;
long l;
boolean eq2;
decimal d;
boolean eq3;
number n;
boolean eq4;
boolean eq5;
string s;
string s1;
boolean eq6;
date mydate;
date anothermydate;
date mydateandtime;
boolean eq7;
boolean eq8;
boolean eq9;

function integer transform() {
	i=10;
	print_err("i="+i);
	
	j=9;
	print_err("j="+j);
	
	eq1=(i>j);
	print_err("eq1="+eq1);
	
	l=10;
	print_err("l="+l);
	
	eq2=(l>=j);
	print_err("eq2="+eq2);
	
	d=10;
	print_err("d="+d);
	
	eq3=d=>i;
	print_err("eq3="+eq3);
	
	n=10;
	print_err("n="+n);
	
	eq4=n.gt.l;
	print_err("eq4="+eq4);
	
	eq5=n.ge.d;
	print_err("eq5="+eq5);
	
	s='hello';
	print_err("s="+s);
	
	s1="hello";
	print_err("s1="+s1);
	
	eq6=s<s1;
	print_err("eq6="+eq6);
	
	mydate=2006-01-01;
	print_err("mydate="+mydate);
	
	anothermydate=2008-03-05;
	print_err("anothermydate="+anothermydate);
	
	mydateandtime=2006-01-01 15:30:00;
	print_err("mydateandtime="+mydateandtime);
	
	eq7 = mydate < mydateandtime; 
	print_err("eq7="+eq7);
	
	eq8=mydate .lt. anothermydate;
	print_err("eq8="+eq8);
	
	anothermydate=2006-1-1 0:0:0;print_err("anothermydate="+anothermydate);
	eq9=mydate<=anothermydate;
	print_err("eq9="+eq9);
	
	return 0;
}