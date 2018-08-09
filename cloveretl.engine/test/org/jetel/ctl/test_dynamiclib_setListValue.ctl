string[] strList = ["John", "Doe", "Jersey"];
integer[] intList = [123, 456, 789];
date[] dateList = [str2date("1970-01-01 01:00:12", "yyyy-MM-dd HH:mm:ss"), str2date("1970-01-01 01:00:34", "yyyy-MM-dd HH:mm:ss")];
//byte[] byteList = [hex2byte("0x12, 0x34"),hex2byte("0x56, 0x78")];
byte[] byteList;
decimal[] decimalList = [12.34D, 56.78D];

string[] ret1;
string[] ret2;
string[] ret3;
string[] ret4;
string[] ret5;

function integer transform(){
	append(byteList, str2byte("aa", "UTF-8"));
	
	append(byteList, str2byte("bb", "UTF-8"));
	
	setListValue($out.4, "stringListField", strList);
	setListValue($out.4, "integerListField", intList);
	setListValue($out.4, "dateListField", dateList);
	setListValue($out.4, "byteListField", byteList);
	setListValue($out.4, "decimalListField", decimalList);
	ret1 = getListValue($out.4, "stringListField");
	ret2 = getListValue($out.4, "integerListField");
	ret3 = getListValue($out.4, "dateListField");
	ret4 = getListValue($out.4, "byteListField");
	ret5 = getListValue($out.4, "decimalListField");

	return 0;
}