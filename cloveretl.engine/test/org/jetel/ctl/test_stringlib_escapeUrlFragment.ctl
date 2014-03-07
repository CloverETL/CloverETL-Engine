string noCharset;

string utf8;
string cp1250;
string defaultCharset;

string keyValue;
string keyValueSpace;

string emptyString;
string nullValue1;
string nullValue2;

function integer transform() {
	noCharset = escapeUrlFragment("nothing_interesting");
	
	string kun = "\u017Elu\u0165ou\u010Dk\u00FD_k\u016F\u0148_\u00FAp\u011Bl_\u010F\u00E1belsk\u00E9_\u00F3dy";
	utf8 = escapeUrlFragment(kun, "UTF-8");
	cp1250 = escapeUrlFragment(kun, "Windows-1250");
	defaultCharset = escapeUrlFragment(kun);

	keyValue = escapeUrlFragment("name=value"); // name%3Dvalue
	keyValueSpace = escapeUrlFragment("name=long value"); // name%3Dlong+value
	
	emptyString = escapeUrlFragment("");
	nullValue1 = escapeUrlFragment(null);
	string value = null;
	nullValue2 = escapeUrlFragment(value);
	
	return 0;
}