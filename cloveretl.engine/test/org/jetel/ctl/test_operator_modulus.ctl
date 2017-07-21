integer i;
integer j;
integer imoduloj;
long l;
long m;
long lmodulom;
long mmoduloi;
long imodulom;
number n;
number m1;
number nmodulom1;
number nmoduloj;
number jmodulon;
number m1modulom;
number mmodulom1;
decimal d;
decimal d1;
decimal dmodulod1;
decimal dmoduloj;
decimal jmodulod;
decimal dmodulom;
decimal mmodulod;
decimal dmodulon;
decimal nmodulod;

function integer transform() {
	i=10;
	j=100;
	imoduloj=i%j;
	printErr("modulo integer:"+imoduloj);

	l=0x7fffffffl/10l;
	printErr(l);

	m=0x7fffffffl;
	printErr(m);

	lmodulom=l%m;
	printErr("modulo long:"+lmodulom);

	mmoduloi=m%i;
	printErr("long modulo integer:"+mmoduloi);

	imodulom=i%m;
	printErr("integer modulo long:"+imodulom);

	n=0.1;
	printErr(n);

	m1=0.001;
	printErr(m1);

	nmodulom1=n%m1;
	printErr("modulo number:"+nmodulom1);

	nmoduloj=n%j;
	printErr("number modulo integer:"+nmoduloj);

	jmodulon=j%n;
	printErr("integer modulo number:"+jmodulon);

	m1modulom=m1%m;
	printErr("number modulo long:"+m1modulom);

	mmodulom1=m%m1;
	printErr("long modulo number:"+mmodulom1);

	d=0.1D;
	printErr(d);

	d1=0.0001D;
	printErr(d1);

	dmodulod1=d%d1;
	printErr("modulo decimal:"+dmodulod1);

	dmoduloj=d%j;
	printErr("decimal modulo integer:"+dmoduloj);

	jmodulod=j%d;
	printErr("integer modulo decimal:"+jmodulod);

	dmodulom=d%m;
	printErr("decimal modulo long:"+dmodulom);

	mmodulod=m%d;
	printErr("long modulo decimal:"+mmodulod);

	dmodulon=d%n;
	printErr("decimal modulo number:"+dmodulon);

	nmodulod=n%d;
	printErr("number modulo decimal:"+nmodulod);

	return 0;
}