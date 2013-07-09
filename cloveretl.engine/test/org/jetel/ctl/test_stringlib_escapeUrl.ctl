string escaped;
string unescaped;

function integer transform() {
	string url = "http://example.com/foo bar^";
	escaped = escapeUrl(url);
	unescaped = unescapeUrl(escaped);
	
	return 0;
}