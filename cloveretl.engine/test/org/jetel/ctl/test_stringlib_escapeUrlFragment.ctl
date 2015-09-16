string noCharset;

string utf8;
string cp1250;
string defaultCharset;

string keyValue;
string keyValueSpace;

string emptyString;
string nullValue1;
string nullValue2;

string emailAddressLogin1;
string emailAddressLogin2;

string hashLogin1;
string hashLogin2;

string plusLogin1;
string plusLogin2;

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
	
	// CLO-2367
	emailAddressLogin1 = escapeUrlFragment("http://some.user@gooddata.com:password@secure.bestdata.com");
	emailAddressLogin2 = "http://" + escapeUrlFragment("some.user@gooddata.com") + ":password@secure.bestdata.com";
	
	hashLogin1 = escapeUrlFragment("http://ac#dsds.dsz:password@server.goooddata.com/nice");
	hashLogin2 = "http://" + escapeUrlFragment("ac#dsds.dsz") + ":password@server.goooddata.com/nice";
	
	plusLogin1 = escapeUrlFragment("http://ac%+dsds.dsz:password@server.goooddata.com/nice");
	plusLogin2 = "http://" + escapeUrlFragment("ac%+dsds.dsz") + ":password@server.goooddata.com/nice";
	
	return 0;
}