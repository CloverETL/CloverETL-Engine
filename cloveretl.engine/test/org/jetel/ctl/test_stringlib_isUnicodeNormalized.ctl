boolean nullValue1;
boolean nullValue2;
boolean emptyString;

boolean nfd;
boolean nfc;
boolean nfkd;
boolean nfkc;

boolean NFD;
boolean NFC;
boolean NFKD;
boolean NFKC;

boolean[] results;

function integer transform() {
	nullValue1 = isUnicodeNormalized(null, "NFD");
	string nullValue = null;
	nullValue2 = isUnicodeNormalized(nullValue, "NFC");
	emptyString = isUnicodeNormalized("", "NFKC");
	
	nfd = isUnicodeNormalized("A\u030AA\u030AA\u030A\u0132c\u030Cc\u030C", "nfd");
	nfc = isUnicodeNormalized("\u00C5\u00C5\u00C5\u0132\u010D\u010D", "nfc");
	nfkd = isUnicodeNormalized("A\u030AA\u030AA\u030AIJc\u030Cc\u030C", "nfkd");
	nfkc = isUnicodeNormalized("\u00C5\u00C5\u00C5IJ\u010D\u010D", "nfkc");
	
	NFD = isUnicodeNormalized("A\u030AA\u030AA\u030A\u0132c\u030Cc\u030C", "NFD");
	NFC = isUnicodeNormalized("\u00C5\u00C5\u00C5\u0132\u010D\u010D", "NFC");
	NFKD = isUnicodeNormalized("A\u030AA\u030AA\u030AIJc\u030Cc\u030C", "NFKD");
	NFKC = isUnicodeNormalized("\u00C5\u00C5\u00C5IJ\u010D\u010D", "NFKC");
	
	string input = "A\u030A\u00C5\u212B\u0132c\u030C\u010D"; // A, combining ring above, A_ring, Angstrom, ij, c, combining caron, c_caron
	string[] algorithms = ["NFD", "NFC", "NFKD", "NFKC"];
	for (integer i = 0; i < algorithms.length(); i++) {
		string algorithm = algorithms[i];
		results.push(isUnicodeNormalized(input, algorithm.upperCase()));
		results.push(isUnicodeNormalized(input, algorithm.lowerCase()));
	}
	
	return 0;
}