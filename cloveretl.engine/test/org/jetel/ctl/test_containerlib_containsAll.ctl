boolean[] results;

boolean test1;
boolean test2;
boolean test3;
boolean test4;
boolean test5;
boolean test6;
boolean test7;
boolean test8;
boolean test9;
boolean test10;
boolean test11;
boolean test12;
boolean test13;
function integer transform() {
	results[0] = $in.multivalueInput.stringListField.containsAll(["John", "Doe", "John"]);
	results[1] = $in.multivalueInput.stringListField.containsAll(["John", "Doe", null]);
	results[2] = $in.multivalueInput.stringListField.containsAll(["John", "Doe", "Jersey"]);
	results[3] = $in.multivalueInput.stringListField.containsAll(["John", "Dee"]);
	
	integer[] emptyList;
	results[4] = $in.multivalueInput.integerListField.containsAll(emptyList);
	results[5] = $in.multivalueInput.integerListField.containsAll([123]);
	results[6] = $in.multivalueInput.integerListField.containsAll([789, 456]);
	results[7] = $in.multivalueInput.integerListField.containsAll([1234, 123]);
	results[8] = $in.multivalueInput.integerListField.containsAll([789, 123, 456]);
	results[9] = $in.multivalueInput.integerListField.containsAll([789, 123, 456, 123]);
	results[10] = $in.multivalueInput.integerListField.containsAll([789, 123, 4567, 123]);
	
	string[] stringList =['look',null];
	string[] forTest2;
	string str = null;
	append(forTest2, str);
	test1 = stringList.containsAll(['look']);
	test2 = stringList.containsAll(forTest2);
	test3 = stringList.containsAll([null,'look']);
	test4 = stringList.containsAll(['']);
	
	long[] longList =[12l,19L,null];
	test5 = longList.containsAll([12L, null]); 
	test6 = longList.containsAll([123L]);
	
	byte byteTest = str2byte('smile','utf-8');
	byte[] byteList =[str2byte('chester','utf-8'),byteTest, null];
	test7 = byteList.containsAll([null,byteTest]);
	test8 = byteList.containsAll([str2byte('smile','utf-16')]);

	number[] numberList = [2.36,null,56.98];
	test9 = numberList.containsAll([2.36,null]);
	test10 = numberList.containsAll([98.7]);
	
	decimal[] decList = [89.6d, 5.3d,null];
	test11 = decList.containsAll([null,5.3d]);
	test12 = decList.containsAll([12.3d]);
	
	test13 = emptyList.containsAll([12, 32]);
	
	return 0;
}