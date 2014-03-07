integer index1;
integer index2;
integer index3;
integer index4;
integer index5;
integer index6;
integer index7;
integer index8;
integer index9;
integer index10;

integer nullLiteral1;
integer nullLiteral2;
integer nullVariable1;
integer nullVariable2;

function integer transform() {
	index1 = "012abc678abc".lastIndexOf("abc");
	index2 = "012abc678abc".lastIndexOf("abc", 200);
	index3 = "012abc678abc".lastIndexOf("abc", 7);

	index4 = "012abc678abc".lastIndexOf("");
	index5 = "012abc678abc".lastIndexOf("", 200);
//	index6 = "012abc678abc".lastIndexOf(null);
//	index7 = "012abc678abc".lastIndexOf(null, 200);
	
	index8 = "".lastIndexOf("");
	index9 = "".lastIndexOf("", 10);
	index10 = "012abc678abc".lastIndexOf("abc", -200);
	
	nullLiteral1 = lastIndexOf(null, "abc");
	nullLiteral2 = lastIndexOf(null, "abc", 10);
	string nullValue = null;
	nullVariable1 = lastIndexOf(nullValue, "abc");
	nullVariable2 = lastIndexOf(nullValue, "abc", 10);
	
	return 0;
}