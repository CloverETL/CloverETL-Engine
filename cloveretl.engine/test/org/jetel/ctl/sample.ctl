public int transform() {
	$0.FullName = ($FirstName + " ") + $LastName;
	$0.DateOfBirth = $0.DateOfBirth;
}

--------------------------[ $0.FullName = ($FirstName + " ") + $LastName; ]------------
									=
						FldAcc				(+ out)
										 (+ in)		FldAcc
									FldAcc		" "		

	
	( LHS )
	[
		out[0].getField(FullName).setValue()
	] 
	
	( op ) = 
	
	( RHS ) 
	[ 
		[  
			[
				in[0].getField(FirstName).getValue();
			]
			 ( + in )
			[
				" "
			]		  
		
		] 
		( + out )
		[  
			in[0].getField(LastName).getValue()			
		]
		
		
	]

----------------------------[ $0.DateOfBirth = $0.DateOfBirth;]----------------

				=
		FldAcc		FldAcc
		
 [
 	out[0].getField(DateOfBirth).setValue(?)
 ]
 
 ( = )
 [
 	(Date)in[0].getField(DateOfBirth).getValue()
 ]

public int transform() {
	in[0].getField(0).setValue([ (String)in[0].getField(FirstName).getValue() + " " + (String)in[0].getField(LastName).getValue()]);
	out[0].getField(DateOfBirth).setValue((Date)in[0].getField(DateOfBirth).getValue());
}


====================================================================================

public int transform() {
	decimal d = 1000;
	$0.Sum = $0.Sum + (d++);
}

						VarDecl
								=
									1000
									
--------------------------[	decimal d = 1000; ]---------------------

[
	Decimal d = DecimalFactory.createDecimal(8,2);
	d.setValue(?)
]
(=)
[
	1000
]
--------------------------[ $0.Sum = $0.Sum + (d++); ]-----------------


[
	out[0].getField(Sum).setValue(?)
]
(=)
[
	[
		in[0].getField(Sum).getValue() <-
	]
	( + )
	[
		(++)
		[
			Decimal z1 = d.duplicateValue();
			z1.add(1);
			z1.getValue() <-
		]
	]
	{
		(+) is decimal-type plus
		Decimal z2 = DecimalFactory.createDecimal(8,2);
		z2.setValue(in[0].getField(Sum).getValue());
		Decimal z1 = d.duplicateValue();
		z1.add(1);
		z2.add(z1.getValue());
		
		z2.getValue() <-

	}
]

public int transform() {
	// decimal d = 1000;
	Decimal d = DecimalFactory.createDecimal(8,2);
	d.setValue(1000);

	// $0.Sum = $0.Sum + (d++);
	Decimal z2 = DecimalFactory.createDecimal(8,2);
	z2.setValue(in[0].getField(Sum).getValue());
	Decimal z1 = d.duplicateValue();
	z1.add(1);
	z2.add(z1.getValue());
	out[0].getField(Sum).setValue(z2.getValue())
}		