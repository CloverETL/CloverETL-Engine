integer i;
integer j;
boolean eq0;
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
boolean eq7;
date mydate;
date anothermydate;
boolean eq8;
boolean eq9;
boolean eq10;

function integer transform() {
	i=10;
	printErr('i='+i);
	
	j=9;
	printErr('j='+j);
	
	eq0=(i.eq.(j+1));
	printErr('eq0: i==j+1 '+eq0);
	
	eq1=(i==j+1);
	printErr('eq1: i==j+1 '+eq1);
	
	l=10;
	printErr('l='+l);
	
	eq2=(l==j);
	printErr('eq2: l==j '+eq2);
	
	eq2=(l.eq.i);
	printErr('eq2: l==i ' + eq2);
	
	d=10;
	printErr('d='+d);
	
	eq3=d==i;
	printErr('eq3: d==i '+eq3);
	
	n=10;
	printErr('n='+n);
	
	eq4=n.eq.l;
	printErr('eq4 n==l '+eq4);
	
	eq5=n==d;
	printErr('eq5: n==d '+eq5);
	
	s='hello';
	printErr('s='+s);
	
	s1='hello ';
	printErr('s1='+s1);
	
	eq6=s.eq.s1;
	printErr('eq6 s==s1 '+eq6);
	
	eq7=s==trim(s1);
	printErr('eq7 s==trim(s1)'+eq7);
	
	mydate=2006-01-01;
	printErr('mydate='+mydate);
	printErr('anothermydate='+anothermydate);
	
	eq8=mydate.eq.anothermydate;
	printErr('eq8: mydate == anothermydate '+eq8);
	
	anothermydate=2006-1-1 0:0:0;
	printErr('anothermydate='+anothermydate);
	
	eq9=mydate==anothermydate;
	printErr('eq9: mydate == anothermydate ='+eq9);
	
	eq10=eq9.eq.eq8;
	printErr('eq10: eq9 == eq8 '+eq10);
	
	return 0;
}