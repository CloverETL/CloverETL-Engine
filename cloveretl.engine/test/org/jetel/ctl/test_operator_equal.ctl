integer i;
integer j;
integer k;
boolean eq0;
boolean eq1;
boolean eq1a;
boolean eq1b;
boolean eq1c;
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
boolean eq11;
boolean eq12;
boolean eq13;
boolean eq14;
boolean eq15;
boolean eq16;
boolean eq17;
boolean eq18;
boolean eq19;

function integer getInt() {
	return 1000;
}

function integer transform() {

	//integer
	i=1000; // it is better to test it with higher value then 127, since the less Integer values are cached by JVM
	printErr('i='+i);
	
	j=999;
	printErr('j='+j);
	
	eq0=(i.eq.(j+1));
	printErr('eq0: i==j+1 '+eq0);
	
	eq1=(i==j+1);
	printErr('eq1: i==j+1 '+eq1);

	eq1a = (getInt()==i);
	printErr('eq1a: getInt()==i ' + eq1a);

	eq1b = (getInt()==j+1);
	printErr('eq1a: getInt()==j+1 ' + eq1b);

	eq1c = (j==getInt());
	printErr('eq1a: j==getInt() ' + eq1c);

	k = null;
	eq13 = k==null;
	printErr('eq13: k==null ' + eq13);
	
	eq14 = j==null;
	printErr('eq14: j==null ' + eq14);

	eq19 = j==k;
	printErr('eq19: j==null ' + eq19);
	
	
	//long
	l=1000;
	printErr('l='+l);
	
	eq2=(l==j);
	printErr('eq2: l==j '+eq2);
	
	eq2=(l.eq.i);
	printErr('eq2: l==i ' + eq2);
	
	
	//decimal
	d=1000;
	printErr('d='+d);
	
	eq3=d==i;
	printErr('eq3: d==i '+eq3);
	
	
	//number
	n=1000;
	printErr('n='+n);
	
	eq4=n.eq.l;
	printErr('eq4 n==l '+eq4);
	
	eq5=n==d;
	printErr('eq5: n==d '+eq5);
	
	
	//string
	s='hello';
	printErr('s='+s);
	
	s1='hello ';
	printErr('s1='+s1);
	
	eq6=s.eq.s1;
	printErr('eq6 s==s1 '+eq6);
	
	eq7=s==trim(s1);
	printErr('eq7 s==trim(s1)'+eq7);
	
	eq15 = null==s;
	printErr('eq15: null==s ' + eq15);
	
	s = null;
	eq16 = null==s;
	printErr('eq16: null==s ' + eq16);
	
	//date
	mydate=2006-01-01;
	printErr('mydate='+mydate);
	printErr('anothermydate='+anothermydate);
	
	eq8=mydate.eq.anothermydate;
	printErr('eq8: mydate == anothermydate '+eq8);
	
	anothermydate=2006-1-1 0:0:0;
	printErr('anothermydate='+anothermydate);
	
	eq9=mydate==anothermydate;
	printErr('eq9: mydate == anothermydate ='+eq9);

	eq17 = mydate==null;
	printErr('eq17: mydate==null ' + eq17);

	mydate = null;
	eq18 = mydate!=null;
	printErr('eq18: mydate!=null ' + eq18);
	

	//boolean
	eq10=eq9.eq.eq8;
	printErr('eq10: eq9 == eq8 '+eq10);
	
	
	//null
	eq11 = null==null;	
	printErr('eq11: null==null ' + eq11);

	eq12 = null!=null;	
	printErr('eq12: null!=null ' + eq12);
	
	
	return 0;
}