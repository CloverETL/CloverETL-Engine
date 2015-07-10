map[string, string] ret1;
map[string, string] ret2;
map[string, string] ret3;
map[string, string] ret4;
map[string, string] ret5;
map[string, string] ret6;

function integer transform() {
	parseProperties("xxx=1\nkkk=2\naaa=3\nbbb=4").clear(); // the map should be modifiable
	ret1 = parseProperties("xxx=1\nkkk=2\naaa=3\nbbb=4"); // the same input as before (literal); clearing the first map must not affect the second result

	ret2 = parseProperties(""); // empty string
	ret3 = parseProperties("   "); // blank string
	ret4 = parseProperties(null); // blank string
	ret5 = parseProperties("include=aaa"); // "include" key


	return 0;
}