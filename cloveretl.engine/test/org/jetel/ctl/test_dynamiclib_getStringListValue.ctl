string[] ret1;
string[] ret2;
string[] ret3;
string[] ret4;
string[] ret5;


function integer transform(){
	ret1 = getStringListValue($in.3, "stringListField");
	ret2 = getStringListValue($in.3, "integerListField");
	ret3 = getStringListValue($in.3, "dateListField");
	ret4 = getStringListValue($in.3, "byteListField");
	ret5 = getStringListValue($in.3, "decimalListField");

	return 0;
}