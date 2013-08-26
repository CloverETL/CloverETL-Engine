string ret1;
string ret2;
string ret3;
string ret4;
string ret5;
string ret6;
string ret7;
string ret8;

function integer transform(){
	string xmlChunk = '<lastName>Smith</lastName><phoneNumber><number>212 555-1234</number><type>home</type></phoneNumber><phoneNumber><number>646 555-4567</number><type>fax</type></phoneNumber><address><streetAddress>21 2nd Street</streetAddress><postalCode>10021</postalCode><state>NY</state><city>New York</city></address><age>25</age><firstName>John</firstName>';
	ret1 = xml2json(xmlChunk);
	ret2 = xml2json('<name>Renektor</name>');
	ret3 = xml2json('');
	ret4 = xml2json('<address/>');
	ret5 = xml2json('<age>32</age>');
	ret6 = xml2json('<b/>');
	ret7 = xml2json('<char><name>Anivia</name><lane>mid</lane></char>');
	ret8 = xml2json('<#>/</#>');
	
	return 0;
}