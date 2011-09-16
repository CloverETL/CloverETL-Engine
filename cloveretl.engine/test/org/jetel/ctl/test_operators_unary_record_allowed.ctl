decimal[] currency;
integer[] value;
long[] bornMillisec;
number[] age;

function integer transform() {
	
	// integer
	$out.0.Value = 14;
	value[0] = $out.0.Value++;
	value[1] = ++$out.0.Value;
	value[2] = $out.0.Value;
	$out.1.Value = 65;
	value[3] = $out.1.Value--;
	value[4] = --$out.1.Value;
	value[5] = $out.1.Value;
	
	// long
	$out.0.BornMillisec = 14;
	bornMillisec[0] = $out.0.BornMillisec++;
	bornMillisec[1] = ++$out.0.BornMillisec;
	bornMillisec[2] = $out.0.BornMillisec;
	$out.1.BornMillisec = 65;
	bornMillisec[3] = $out.1.BornMillisec--;
	bornMillisec[4] = --$out.1.BornMillisec;
	bornMillisec[5] = $out.1.BornMillisec;
	
	// number
	$out.0.Age = 14.123;
	age[0] = $out.0.Age++;
	age[1] = ++$out.0.Age;
	age[2] = $out.0.Age;
	$out.1.Age = 65.789;
	age[3] = $out.1.Age--;
	age[4] = --$out.1.Age;
	age[5] = $out.1.Age;
	
	// decimal 
	$out.0.Currency = 12.5;
	currency[0] = $out.0.Currency++; 
	currency[1] = ++$out.0.Currency;
	currency[2] = $out.0.Currency;
	$out.1.Currency = 65.432;
	currency[3] = $out.1.Currency--; 
	currency[4] = --$out.1.Currency;
	currency[5] = $out.1.Currency;
	
	return 0;
}