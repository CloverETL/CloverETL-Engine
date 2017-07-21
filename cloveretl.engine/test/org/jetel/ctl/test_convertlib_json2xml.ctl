string ret;
string ret2;
string ret3;
string ret4;
string ret5;
string ret6;
string ret7;
string ret8;
function integer transform(){
	string json = '{"firstName": "John","lastName": "Smith","age": 25,"address": {"streetAddress": "21 2nd Street","city": "New York","state": "NY","postalCode": "10021"},"phoneNumber": [{"type": "home","number": "212 555-1234"},{"type": "fax", "number": "646 555-4567"}]}';
	ret = json2xml(json);
	ret2 = json2xml('{"name":""}');
	ret3 = json2xml('{"address":{}}');
	ret4 = json2xml('{"":""}');
	ret5 = json2xml('{"#":""}');
	ret6 = json2xml('{"/":"/"}');
	ret7 = json2xml('{}');
	ret8 = json2xml('{"":"Urgot"}');
	printErr(ret8);
	return 0;
}