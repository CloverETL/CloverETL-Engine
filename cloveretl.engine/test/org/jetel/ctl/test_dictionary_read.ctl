string s;
integer i;
long l;
decimal d;
number n;
date a;
boolean b;

string sNull;
integer iNull;
long lNull;
decimal dNull;
number nNull;
date aNull;
boolean bNull;

function integer transform() {

	s = dictionary.sVerdon;
	i = dictionary.i211;
	l = dictionary.l452 / 2;
	d = cubed( dictionary.d621 );
	n = dictionary.n9342;
	a = dictionary.a1992;
	b = ( dictionary.bTrue == true);

	sNull = dictionary.s;
	iNull = dictionary.i;
	lNull = dictionary.l;
	dNull = dictionary.d;
	nNull = dictionary.n;
	aNull = dictionary.a;
	bNull = dictionary.b;

	return 0;
}

function decimal cubed(decimal i) {
	return i*i*i;
}