string leaveMe;
string correct;
string clock;

string russian;
string japanese;
string german;

string nullValue;
string emptyString;

function integer transform() {
	leaveMe = uncapitalizeWords("lEAVE mE aLONE");
	correct = uncapitalizeWords("This is correct");
	clock = uncapitalizeWords("7 O'Clock");
	
	russian = uncapitalizeWords("\u041F\u0440\u043E\u043B\u0435\u0442\u0430\u0440\u0438\u0438 \u0412\u0441\u0435\u0445 \u0421\u0442\u0440\u0430\u043D, \u0421\u043E\u0435\u0434\u0438\u043D\u044F\u0439\u0442\u0435\u0441\u044C!");
	japanese = uncapitalizeWords("\u673A\u306E\u4E0A\u306B\u306F\u30B1\u30FC\u30AD\u304C\u3042\u308A\u307E\u3059\u3002");
	german = uncapitalizeWords("\u00DCberwald");
	
	nullValue = uncapitalizeWords(null);
	emptyString = uncapitalizeWords("");
	
	return 0;
}