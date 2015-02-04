byte utf8Hello;
byte utf8Horse;
byte utf8Math;
byte utf16Hello;
byte utf16Horse;
byte utf16Math;
byte macHello;
byte macHorse;
byte asciiHello;
byte isoHello;
byte isoHorse;
byte cpHello;
byte cpHorse;

byte nullLiteralOutput;
byte nullVariableOutput;

function integer transform() {

    string hello = "Hello World!";
    string horse = "Příliš žluťoučký kůň pěl ďáblské ódy";
    string math = "½ ⅓ ¼ ⅕ ⅙ ⅛ ⅔ ⅖ ¾ ⅗ ⅜ ⅘ € ² ³ † × ← → ↔ ⇒ … ‰ Α Β – Γ Δ € Ε Ζ π ρ ς σ τ υ φ χ ψ ω";

	utf8Hello = str2byte(hello, "utf-8");
	utf8Horse = str2byte(horse, "utf-8");
	utf8Math = str2byte(math, "utf-8");

	utf16Hello = str2byte(hello, "utf-16");
	utf16Horse = str2byte(horse, "utf-16");
	utf16Math = str2byte(math, "utf-16");

	macHello = str2byte(hello, "MacCentralEurope");
	macHorse = str2byte(horse, "MacCentralEurope");

	asciiHello = str2byte(hello, "ascii");

	isoHello = str2byte(hello, "iso-8859-2");
	isoHorse = str2byte(horse, "iso-8859-2");

	cpHello = str2byte(hello, "windows-1250");
	cpHorse = str2byte(horse, "windows-1250");
	
	nullLiteralOutput = str2byte(null, "utf-8");
	string nullVariable = null;
	nullVariableOutput = str2byte(nullVariable, "utf-8");
	
	return 0;
}