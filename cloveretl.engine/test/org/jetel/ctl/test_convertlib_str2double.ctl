double parsedDouble1;
double parsedDouble2;
double parsedDouble3;

function integer transform() {
	parsedDouble1 = str2double("100.13");
	parsedDouble2 = str2double("$123,123,123.123 Millions", "$###,###.# Millions");
	parsedDouble3 = str2double("-350000,01 Kc", "#.# Kc", "cs.CZ");
	return 0;
}