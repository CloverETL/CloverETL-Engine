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
	print_err('i='+i);
	
	j=9;
	print_err('j='+j);
	
	eq0=(i.eq.(j+1));
	print_err('eq0: i==j+1 '+eq0);
	
	eq1=(i==j+1);
	print_err('eq1: i==j+1 '+eq1);
	
	l=10;
	print_err('l='+l);
	
	eq2=(l==j);
	print_err('eq2: l==j '+eq2);
	
	eq2=(l.eq.i);
	print_err('eq2: l==i ' + eq2);
	
	d=10;
	print_err('d='+d);
	
	eq3=d==i;
	print_err('eq3: d==i '+eq3);
	
	n=10;
	print_err('n='+n);
	
	eq4=n.eq.l;
	print_err('eq4 n==l '+eq4);
	
	eq5=n==d;
	print_err('eq5: n==d '+eq5);
	
	s='hello';
	print_err('s='+s);
	
	s1='hello ';
	print_err('s1='+s1);
	
	eq6=s.eq.s1;
	print_err('eq6 s==s1 '+eq6);
	
	eq7=s==trim(s1);
	print_err('eq7 s==trim(s1)'+eq7);
	
	mydate=2006-01-01;
	print_err('mydate='+mydate);
	print_err('anothermydate='+anothermydate);
	
	eq8=mydate.eq.anothermydate;
	print_err('eq8: mydate == anothermydate '+eq8);
	
	anothermydate=2006-1-1 0:0:0;
	print_err('anothermydate='+anothermydate);
	
	eq9=mydate==anothermydate;
	print_err('eq9: mydate == anothermydate ='+eq9);
	
	eq10=eq9.eq.eq8;
	print_err('eq10: eq9 == eq8 '+eq10);
	
	return 0;
}