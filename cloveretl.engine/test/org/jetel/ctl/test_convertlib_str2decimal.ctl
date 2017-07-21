decimal parsedDecimal1;
decimal parsedDecimal2;
decimal parsedDecimal3;

function integer transform() {
	parsedDecimal1 = str2decimal("100.13");
	parsedDecimal2 = str2decimal("$123,123,123.123 Millions", "$###,###.# Millions");
	parsedDecimal3 = str2decimal("-350000,01 Kc", "#.# Kc", "cs.CZ");
	return 0;
}