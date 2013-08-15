decimal parsedDecimal1;
decimal parsedDecimal2;
decimal parsedDecimal3;
decimal parsedDecimal4;
decimal parsedDecimal5;
decimal parsedDecimal6;
decimal parsedDecimal7;
decimal nullRet1;
decimal nullRet2;
decimal nullRet3;
decimal nullRet4;
decimal nullRet5;
decimal nullRet6;
decimal nullRet7;
decimal nullRet8;
decimal nullRet9;
decimal nullRet10;
decimal nullRet11;
decimal nullRet12;
decimal nullRet13;
decimal nullRet14;
decimal nullRet15;
decimal nullRet16;

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
	
	string s = null;
	nullRet1 = str2decimal(s);
	nullRet2 = str2decimal(null);
	nullRet3 = str2decimal(s, '###.#');
	nullRet4 = str2decimal(null, '##,##');
	nullRet5 = str2decimal(s, '###.#', 'cs.CZ');
	nullRet6 = str2decimal(null, '##,##', 'en.US');
	nullRet7 = str2decimal('5.05 h', '#.## h', null);
	//CLO-1614
	nullRet8 = str2decimal('5.05', null, null);
	nullRet9 = str2decimal('5.05', null);
	nullRet10 = str2decimal('5.05', s);
	nullRet11 = str2decimal('5.05', s, s);
	nullRet12 = str2decimal('5.05', null, s);
	nullRet13 = str2decimal('5.05', s, null);
 	nullRet14 = str2decimal('5,05', s, 'cs.CZ');
	nullRet15 = str2decimal('5.05', null, 'en.US');
	nullRet16 = str2decimal('5,05', null, 'cs.CZ');
	printErr(nullRet7);
	return 0;
}