integer maxJapanese;

string[] equation;

function integer transform() {

	// all characters are from the BMP plane => the value of "maxJapanese" will be 1
	string japaneseText = "\u673A\u306E\u4E0A\u306B\u306F\u30B1\u30FC\u30AD\u304C\u3042\u308A\u307E\u3059\u3002";
	maxJapanese = 1;
	for (integer i = 0; i < length(japaneseText); ) {
		integer c = japaneseText.codePointAt(i);
		integer l = codePointLength(c);
		maxJapanese = max(l, maxJapanese);
		i = i + l;
	}
	
	// three trans-BMP characters
	string equationText = "\uD835\uDC9C = {\uD835\uDC65, \uD835\uDCCE}"; // A = {x, y}
	// printErr(equationText);
	for (integer i = 0; i < length(equationText); ) {
		integer c = equationText.codePointAt(i);
		integer l = codePointLength(c);
		if (l > 1) {
			equation.push(codePointToChar(c));
		}
		i = i + l;
	}
	
	return 0;
}