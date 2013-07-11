string metaphone1;
string metaphone2;
string metaphone3;
string metaphone4;
string metaphone5;
string test_empty1;
string test_empty2;
string test_null1;
string test_null2;
string aa;

function integer transform() {
	metaphone1 = metaphone('CHRISSIE');
	metaphone2 = metaphone("Gwendoline", 10);
	metaphone3 = metaphone("Gwendoline");
	metaphone4 = metaphone('aa', 0);
	metaphone5 = metaphone('aa', -6);
	test_empty1 = metaphone('');
	test_empty2 = metaphone('',2);
	test_null1 = metaphone(null);
	test_null2 = metaphone(null,6);
	
	return 0;
}