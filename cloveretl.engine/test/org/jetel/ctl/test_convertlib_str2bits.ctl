byte textAsBits1;
byte textAsBits2;
byte textAsBits3;
byte test_empty;
function integer transform() {
	textAsBits1 = str2bits('00000000');
	textAsBits2 = str2bits('11111111');
	textAsBits3 = str2bits('0101000001001101101');
	test_empty = str2bits('');
	return 0;
}