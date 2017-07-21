string reversed1;
string reversed2;
string reversed3;
string nullStr=null;
string czechString;
string nonBMP;
string compositeCharacters1;
string compositeCharacters2;
string compositeCharacters3;
string emptyStr;
string singleChar;

function integer transform() {
	reversed1=reverse("abcdefgh");
	reversed2=reverse("a");
	reversed3=reverse(nullStr);
	
	czechString = reverse("yd\u00F3 \u00E9ksleb\u00E1\u010F l\u011Bp\u00FA \u0148\u016Fk \u00FDk\u010Duo\u0165ul\u017E");
	
	nonBMP = reverse('\uD835\uDC9E\uD83D\uDE04\u29EF\u29F0\u29ED\u29EC\u4E0D\u4E30\u4E92\u4E94\u5345\uD840\uDC0B\u3402\u4E21\u4E27\u4E26\u4DB5');
	
	compositeCharacters1 = reverse('o\u00E1\u00E9');
	compositeCharacters2 = reverse('o\u0301a\u0301e');
	compositeCharacters3 = unicodeNormalize(reverse(unicodeNormalize('o\u0301a\u0301e', "nfc")), "nfd");
	
	emptyStr = reverse('');
	singleChar = reverse('c');

	return 0;
}