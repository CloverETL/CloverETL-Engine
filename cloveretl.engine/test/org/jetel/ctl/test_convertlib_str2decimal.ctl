decimal parsedDecimal1;
decimal parsedDecimal2;
decimal parsedDecimal3;
decimal parsedDecimal4;
decimal parsedDecimal5;
decimal parsedDecimal6;
decimal parsedDecimal7;
boolean isDecimal1;
boolean isDecimal2;
boolean isDecimal3;
boolean isDecimal4;
boolean isDecimal5;
boolean isDecimal6;
boolean isDecimal7;
decimal nullRet1;
decimal nullRet2;
decimal nullRet3;
decimal nullRet4;
decimal nullRet5;
decimal nullRet6;
decimal parsedDecimal8;
decimal parsedDecimal9;
decimal parsedDecimal10;
decimal parsedDecimal11;
decimal parsedDecimal12;
decimal parsedDecimal13;
decimal parsedDecimal14;
decimal parsedDecimal15;
decimal parsedDecimal16;
decimal parsedDecimal17;
boolean notDecimal1;
boolean notDecimal2;
boolean notDecimal3;
boolean notDecimal4;
boolean notDecimal5;
boolean notDecimal6;
boolean isDecimal8;
boolean isDecimal9;
boolean isDecimal10;
boolean isDecimal11;
boolean isDecimal12;
boolean isDecimal13;
boolean isDecimal14;
boolean isDecimal15;
boolean isDecimal16;
boolean isDecimal17;

function integer transform() {
	parsedDecimal1 = str2decimal("100.13");
	isDecimal1 = isDecimal("100.13");
	parsedDecimal2 = str2decimal("$123,123,123.123 Millions", "$###,###.# Millions");
	isDecimal2 = isDecimal("$123,123,123.123 Millions", "$###,###.# Millions");
	parsedDecimal3 = str2decimal("-350000,01 Kc", "#.# Kc", "cs.CZ");
	isDecimal3 = isDecimal("-350000,01 Kc", "#.# Kc", "cs.CZ");
	
	// IMPORTANT! 
	// In Czech locale, the thousand delimiters must be non-breakable spaces (\u00A0), not ordinary spaces.
	
	parsedDecimal4 = str2decimal("1 000 000", "#,###", "cs.CZ");
	isDecimal4 = isDecimal("1 000 000", "#,###", "cs.CZ");
	parsedDecimal5 = str2decimal("1 000 000,99", "#,###.#", "cs.CZ");
	isDecimal5 = isDecimal("1 000 000,99", "#,###.#", "cs.CZ");
	parsedDecimal6 = str2decimal("$123 123 123,123 Millions", "$###,###.# Millions", "cs.CZ");
	isDecimal6 = isDecimal("$123 123 123,123 Millions", "$###,###.# Millions", "cs.CZ");
	parsedDecimal7 = str2decimal("5.01 CZK", '#.# CZ');
	isDecimal7 = isDecimal("5.01 CZK", '#.# CZ');
	printErr(parsedDecimal7);
	
	string s = null;
	nullRet1 = str2decimal(s);
	notDecimal1 = isDecimal(s);
	nullRet2 = str2decimal(null);
	notDecimal2 = isDecimal(null);
	nullRet3 = str2decimal(s, '###.#');
	notDecimal3 = isDecimal(s, '###.#');
	nullRet4 = str2decimal(null, '##,##');
	notDecimal4 = isDecimal(null, '##,##');
	nullRet5 = str2decimal(s, '###.#', 'cs.CZ');
	notDecimal5 = isDecimal(s, '###.#', 'cs.CZ');
	nullRet6 = str2decimal(null, '##,##', 'en.US');
	notDecimal6 = isDecimal(null, '##,##', 'en.US');
	
	parsedDecimal8 = str2decimal('5.05 h', '#.## h', null);
	isDecimal8 = isDecimal('5.05 h', '#.## h', null);
	//CLO-1614
	parsedDecimal9 = str2decimal('5.05', null, null);
	isDecimal9 = isDecimal('5.05', null, null);
	parsedDecimal10 = str2decimal('5.05', null);
	isDecimal10 = isDecimal('5.05', null);
	parsedDecimal11 = str2decimal('5.05', s);
	isDecimal11 = isDecimal('5.05', s);
	parsedDecimal12 = str2decimal('5.05', s, s);
	isDecimal12 = isDecimal('5.05', s, s);
	parsedDecimal13 = str2decimal('5.05', null, s);
	isDecimal13 = isDecimal('5.05', null, s);
	parsedDecimal14 = str2decimal('5.05', s, null);
	isDecimal14 = isDecimal('5.05', s, null);
 	parsedDecimal15 = str2decimal('5,05', s, 'cs.CZ');
 	isDecimal15 = isDecimal('5,05', s, 'cs.CZ');
	parsedDecimal16 = str2decimal('5.05', null, 'en.US');
	isDecimal16 = isDecimal('5.05', null, 'en.US');
	parsedDecimal17 = str2decimal('5,05', null, 'cs.CZ');
	isDecimal17 = isDecimal('5,05', null, 'cs.CZ');
	printErr(parsedDecimal8);
	return 0;
}