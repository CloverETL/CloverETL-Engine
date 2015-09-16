integer russian;
integer slovak;
integer[] japanese;
integer[] equation;

function integer transform() {

	russian = codePointAt("\u043F\u0440\u043E\u043B\u0435\u0442\u0430\u0440\u0438\u0438 \u0432\u0441\u0435\u0445 \u0441\u0442\u0440\u0430\u043D, \u0441\u043E\u0435\u0434\u0438\u043D\u044F\u0439\u0442\u0435\u0441\u044C!", 14);
	slovak = codePointAt("\u010Derven\u00FD \u013Eadoborec", 10);

	// all characters are from the BMP plane
	string japaneseText = "\u673A\u306E\u4E0A\u306B\u306F\u30B1\u30FC\u30AD\u304C\u3042\u308A\u307E\u3059\u3002";
	for (integer i = 0; i < length(japaneseText); ) {
		integer c = japaneseText.codePointAt(i);
		japanese.push(c);
		i = i + codePointLength(c);
	}
	
	// three trans-BMP characters
	string equationText = "\uD835\uDC9C = {\uD835\uDC65, \uD835\uDCCE}"; // A = {x, y}
	// printErr(equationText);
	for (integer i = 0; i < length(equationText); ) {
		integer c = equationText.codePointAt(i);
		equation.push(c);
		// printErr(codePointLength(c));
		i = i + codePointLength(c);
	}
	
	return 0;
}