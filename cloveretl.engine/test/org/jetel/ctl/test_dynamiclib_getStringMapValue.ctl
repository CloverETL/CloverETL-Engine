map[string, string] ret1;
map[string, string] ret2;
map[string, string] ret3;
map[string, string] ret4;
map[string, string] ret5;


function integer transform(){
	ret1 = getStringMapValue($in.3, "stringMapField");
	ret2 = getStringMapValue($in.3, "integerMapField");
	ret3 = getStringMapValue($in.3, "dateMapField");
	ret4 = getStringMapValue($in.3, "byteMapField");
	ret5 = getStringMapValue($in.3, "decimalMapField");

	return 0;
}