metadata1 r1;
metadata2 r2;
metadata3 r3;
metadata4 r4;

function integer transform() {
	r1.a = "a";
	r1.b = "b";
	r1.c = "c";
	
	r1.noexactmatch = "noexactmatch";
	r1.ambiguous = "ambiguous";
	r1.AMBIGUOUS = "AMBIGUOUS";
	r1.aMBIGUOUs = "aMBIGUOUs";
	r1.AmbiguouS = "AmbiguouS";
	r1.exactMATCH = "exactMATCH";
	r1.exactMatch = "exactMatch";
	r1.EXACTMATCH = "EXACTMATCH";
	
	r2.* = r1.*;
	
	// CLO-637
	r3.singleInputField = "dummy value";
	r4.* = r3.*;
	
	return 0;
}