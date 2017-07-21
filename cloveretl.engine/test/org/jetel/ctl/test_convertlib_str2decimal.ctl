decimal parsedDecimal1;
decimal parsedDecimal2;
decimal parsedDecimal3;
decimal parsedDecimal4;
decimal parsedDecimal5;
decimal parsedDecimal6;
decimal parsedDecimal7;
function integer transform() {
	parsedDecimal1 = str2decimal("100.13");
	parsedDecimal2 = str2decimal("$123,123,123.123 Millions", "$###,###.# Millions");
	parsedDecimal3 = str2decimal("-350000,01 Kc", "#.# Kc", "cs.CZ");
	
	// IMPORTANT! 
	// In Czech locale, the thousand delimiters must be non-breakable spaces (\u00A0), not ordinary spaces.
	
	parsedDecimal4 = str2decimal("1 000 000", "#,###", "cs.CZ");
	parsedDecimal5 = str2decimal("1 000 000,99", "#,###.#", "cs.CZ");
	parsedDecimal6 = str2decimal("$123 123 123,123 Millions", "$###,###.# Millions", "cs.CZ");
	parsedDecimal7 = str2decimal("5.01 CZK", '#.# CZ');
	printErr(parsedDecimal7);
	return 0;
}