string[] ret1;
string[] ret2;
string[] ret3;
string[] ret4;
string[] ret5;


function integer transform(){
	ret1 = getListValue($in.3, "stringListField");
	ret2 = getListValue($in.3, "integerListField");
	ret3 = getListValue($in.3, "dateListField");
	ret4 = getListValue($in.3, "byteListField");
	ret5 = getListValue($in.3, "decimalListField");

	return 0;
}