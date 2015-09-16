string nullValue1;
string nullValue2;
string emptyString;

string nfd;
string nfc;
string nfkd;
string nfkc;

string NFD;
string NFC;
string NFKD;
string NFKC;

function integer transform() {
	nullValue1 = unicodeNormalize(null, "NFD");
	string nullValue = null;
	nullValue2 = unicodeNormalize(nullValue, "NFC");
	emptyString = unicodeNormalize("", "NFKC");
	
	string input = "A\u030A\u00C5\u212B\u0132c\u030C\u010D"; // A, combining ring above, A_ring, Angstrom, ij, c, combining caron, c_caron
	
	nfd = unicodeNormalize(input, "nfd");
	nfc = unicodeNormalize(input, "nfc");
	nfkd = unicodeNormalize(input, "nfkd");
	nfkc = unicodeNormalize(input, "nfkc");
	
	NFD = unicodeNormalize(input, "NFD");
	NFC = unicodeNormalize(input, "NFC");
	NFKD = unicodeNormalize(input, "NFKD");
	NFKC = unicodeNormalize(input, "NFKC");
	
	return 0;
}