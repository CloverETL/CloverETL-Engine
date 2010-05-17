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
	print_err("modulo integer:"+imoduloj);

	l=0x7fffffffl/10l;
	print_err(l);

	m=0x7fffffffl;
	print_err(m);

	lmodulom=l%m;
	print_err("modulo long:"+lmodulom);

	mmoduloi=m%i;
	print_err("long modulo integer:"+mmoduloi);

	imodulom=i%m;
	print_err("integer modulo long:"+imodulom);

	n=0.1;
	print_err(n);

	m1=0.001;
	print_err(m1);

	nmodulom1=n%m1;
	print_err("modulo number:"+nmodulom1);

	nmoduloj=n%j;
	print_err("number modulo integer:"+nmoduloj);

	jmodulon=j%n;
	print_err("integer modulo number:"+jmodulon);

	m1modulom=m1%m;
	print_err("number modulo long:"+m1modulom);

	mmodulom1=m%m1;
	print_err("long modulo number:"+mmodulom1);

	d=0.1D;
	print_err(d);

	d1=0.0001D;
	print_err(d1);

	dmodulod1=d%d1;
	print_err("modulo decimal:"+dmodulod1);

	dmoduloj=d%j;
	print_err("decimal modulo integer:"+dmoduloj);

	jmodulod=j%d;
	print_err("integer modulo decimal:"+jmodulod);

	dmodulom=d%m;
	print_err("decimal modulo long:"+dmodulom);

	mmodulod=m%d;
	print_err("long modulo decimal:"+mmodulod);

	dmodulon=d%n;
	print_err("decimal modulo number:"+dmodulon);

	nmodulod=n%d;
	print_err("number modulo decimal:"+nmodulod);

	return 0;
}