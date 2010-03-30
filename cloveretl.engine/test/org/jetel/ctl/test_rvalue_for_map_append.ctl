map[int,string] map1;
map[int,string] map2;
map[int,string] map3;

function int transform () {
	
	map1[1] = "a";
	map1[2] = "b";
	map2[3] = "c";
	map2[4] = "d";
	
	map3 = map1 + map2;
	
	return 0;
}
