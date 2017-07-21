map[integer,string] map1;
map[integer,string] map2;
map[integer,string] map3;

function integer transform () {
	
	map1[1] = "a";
	map1[2] = "b";
	map2[3] = "c";
	map2[4] = "d";
	
	map3 = map1 + map2;
	
	return 0;
}
