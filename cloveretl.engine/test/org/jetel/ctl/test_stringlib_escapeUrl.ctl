string escaped;
string unescaped;

string unesc_empty;
string unesc_null;

string esc_empty;
string esc_null;

function integer transform() {
	string url = "http://example.com/foo bar^";
	escaped = escapeUrl(url);
	unescaped = unescapeUrl(escaped);
	
//	unesc_empty = unescapeUrl('');
//	unesc_null = unescapeUrl(null);
	
//	esc_empty = escapeUrl('');
//	esc_null = escapeUrl(null);
	
	return 0;
}