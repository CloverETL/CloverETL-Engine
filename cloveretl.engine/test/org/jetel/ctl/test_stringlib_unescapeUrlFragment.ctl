string noCharset;

string utf8;
string cp1250;
string defaultCharset;

string keyValue;
string keyValueSpace1;
string keyValueSpace2;

string emptyString;
string nullValue1;
string nullValue2;

function integer transform() {
	noCharset = unescapeUrlFragment("nothing_interesting");
	
	utf8 = unescapeUrlFragment("%C5%BElu%C5%A5ou%C4%8Dk%C3%BD_k%C5%AF%C5%88_%C3%BAp%C4%9Bl_%C4%8F%C3%A1belsk%C3%A9_%C3%B3dy", "UTF-8");
	cp1250 = unescapeUrlFragment("%9Elu%9Dou%E8k%FD_k%F9%F2_%FAp%ECl_%EF%E1belsk%E9_%F3dy", "Windows-1250");
	// UTF-8
	defaultCharset = unescapeUrlFragment("%C5%BElu%C5%A5ou%C4%8Dk%C3%BD_k%C5%AF%C5%88_%C3%BAp%C4%9Bl_%C4%8F%C3%A1belsk%C3%A9_%C3%B3dy");

	keyValue = unescapeUrlFragment("name%3Dvalue"); // name=value
	keyValueSpace1 = unescapeUrlFragment("name%3Dlong+value"); // name=long value
	keyValueSpace2 = unescapeUrlFragment("name%3Dlong%20value"); // name=long value
	
	emptyString = unescapeUrlFragment("");
	nullValue1 = unescapeUrlFragment(null);
	string value = null;
	nullValue2 = unescapeUrlFragment(value);
	
	return 0;
}