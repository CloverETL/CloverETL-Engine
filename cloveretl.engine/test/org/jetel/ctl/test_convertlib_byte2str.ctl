string utf8Hello;
string utf8Horse;
string utf8Math;
string utf16Hello;
string utf16Horse;
string utf16Math;
string macHello;
string macHorse;
string asciiHello;
string isoHello;
string isoHorse;
string cpHello;
string cpHorse;
string nullLiteralOutput;
string nullVariableOutput;

function integer transform() {

	utf8Hello = byte2str(hex2byte("48656c6c6f20576f726c6421"), "utf-8");
	utf8Horse = byte2str(hex2byte("50c599c3ad6c69c5a120c5be6c75c5a56f75c48d6bc3bd206bc5afc5882070c49b6c20c48fc3a1626c736bc3a920c3b36479"), "utf-8");
	utf8Math = byte2str(hex2byte("c2bd20e2859320c2bc20e2859520e2859920e2859b20e2859420e2859620c2be20e2859720e2859c20e2859820e282ac20c2b220c2b320e280a020c39720e2869020e2869220e2869420e2879220e280a620e280b020ce9120ce9220e2809320ce9320ce9420e282ac20ce9520ce9620cf8020cf8120cf8220cf8320cf8420cf8520cf8620cf8720cf8820cf89"), "utf-8");
	
	utf16Hello = byte2str(hex2byte("feff00480065006c006c006f00200057006f0072006c00640021"), "utf-16");
	utf16Horse = byte2str(hex2byte("feff0050015900ed006c006901610020017e006c00750165006f0075010d006b00fd0020006b016f014800200070011b006c0020010f00e10062006c0073006b00e9002000f300640079"), "utf-16");
	utf16Math = byte2str(hex2byte("feff00bd00202153002000bc00202155002021590020215b0020215400202156002000be002021570020215c00202158002020ac002000b2002000b300202020002000d7002021900020219200202194002021d200202026002020300020039100200392002020130020039300200394002020ac0020039500200396002003c0002003c1002003c2002003c3002003c4002003c5002003c6002003c7002003c8002003c9"), "utf-16");
	
	macHello = byte2str(hex2byte("48656c6c6f20576f726c6421"), "MacCentralEurope");
	macHorse = byte2str(hex2byte("50de926c69e420ec6c75e96f758b6bf9206bf3cb20709e6c209387626c736b8e20976479"), "MacCentralEurope");
	
	asciiHello = byte2str(hex2byte("48656c6c6f20576f726c6421"), "ascii");
	
	isoHello = byte2str(hex2byte("48656c6c6f20576f726c6421"), "iso-8859-2");
	isoHorse = byte2str(hex2byte("50f8ed6c69b920be6c75bb6f75e86bfd206bf9f22070ec6c20efe1626c736be920f36479"), "iso-8859-2");
	
	cpHello = byte2str(hex2byte("48656c6c6f20576f726c6421"), "windows-1250");
	cpHorse = byte2str(hex2byte("50f8ed6c699a209e6c759d6f75e86bfd206bf9f22070ec6c20efe1626c736be920f36479"), "windows-1250");
	
	nullLiteralOutput = byte2str(null, "utf-8");
	byte nullVariable = null;
	nullVariableOutput = byte2str(nullVariable, "utf-8");
	
	return 0;
}