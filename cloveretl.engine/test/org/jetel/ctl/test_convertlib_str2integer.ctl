integer parsedInteger1;
integer parsedInteger2;
integer parsedInteger3;
integer parsedInteger4;

function integer transform() {
	parsedInteger1 = str2integer("123456789");
	parsedInteger2 = str2integer("$123,123 Millions", "$###,###.# Millions");
	parsedInteger3 = str2integer("-350000 Kc", "#.# Kc", "cs.CZ");
	parsedInteger4 = str2integer("JK", 21);
	return 0;
}