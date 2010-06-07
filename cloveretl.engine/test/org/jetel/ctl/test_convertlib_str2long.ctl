long parsedLong1;
long parsedLong2;
long parsedLong3;
long parsedLong4;

function integer transform() {
	parsedLong1 = str2long("1234567890123");
	parsedLong2 = str2long("$123,123,123456789 Millions", "$###,###.# Millions");
	parsedLong3 = str2long("-350000 Kc", "#.# Kc", "cs.CZ");
	parsedLong4 = str2long("B1", 12);
	return 0;
}