string word;
string twoWords;
string sentence;
string clock;
string titleCase;

string underscore;
string slovak;
string russian;
string japanese;
string german;

string nullValue;
string emptyString;

function integer transform() {
	word = capitalizeWords("word");
	twoWords = capitalizeWords("two words");
	sentence = capitalizeWords("A full sentence.");
	clock = capitalizeWords("8 o'clock");
	underscore = capitalizeWords("do _not_ capitalize Me");
	titleCase = capitalizeWords("a fULL sENTENCE");
		
	slovak = capitalizeWords("\u010Derven\u00FD \u013Eadoborec");
	russian = capitalizeWords("\u043F\u0440\u043E\u043B\u0435\u0442\u0430\u0440\u0438\u0438 \u0432\u0441\u0435\u0445 \u0441\u0442\u0440\u0430\u043D, \u0441\u043E\u0435\u0434\u0438\u043D\u044F\u0439\u0442\u0435\u0441\u044C!");
	japanese = capitalizeWords("\u673A\u306E\u4E0A\u306B\u306F\u30B1\u30FC\u30AD\u304C\u3042\u308A\u307E\u3059\u3002");
	german = capitalizeWords("\u00FCberwald");
	
	nullValue = capitalizeWords(null);
	emptyString = capitalizeWords("");
	
	return 0;
}