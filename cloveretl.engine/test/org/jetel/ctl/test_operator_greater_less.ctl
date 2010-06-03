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
	printErr("i="+i);
	
	j=9;
	printErr("j="+j);
	
	eq1=(i>j);
	printErr("eq1="+eq1);
	
	l=10;
	printErr("l="+l);
	
	eq2=(l>=j);
	printErr("eq2="+eq2);
	
	d=10;
	printErr("d="+d);
	
	eq3=d=>i;
	printErr("eq3="+eq3);
	
	n=10;
	printErr("n="+n);
	
	eq4=n.gt.l;
	printErr("eq4="+eq4);
	
	eq5=n.ge.d;
	printErr("eq5="+eq5);
	
	s='hello';
	printErr("s="+s);
	
	s1="hello";
	printErr("s1="+s1);
	
	eq6=s<s1;
	printErr("eq6="+eq6);
	
	mydate=2006-01-01;
	printErr("mydate="+mydate);
	
	anothermydate=2008-03-05;
	printErr("anothermydate="+anothermydate);
	
	mydateandtime=2006-01-01 15:30:00;
	printErr("mydateandtime="+mydateandtime);
	
	eq7 = mydate < mydateandtime; 
	printErr("eq7="+eq7);
	
	eq8=mydate .lt. anothermydate;
	printErr("eq8="+eq8);
	
	anothermydate=2006-1-1 0:0:0;printErr("anothermydate="+anothermydate);
	eq9=mydate<=anothermydate;
	printErr("eq9="+eq9);
	
	return 0;
}