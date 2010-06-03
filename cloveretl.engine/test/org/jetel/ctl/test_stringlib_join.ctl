string joinedString;
string joinedString1;
string joinedString2;
string joinedString3;

function integer transform() {
	joinedString = join(',', "Bagr", 3, 3.5641, -87L, "CTL2");
	
	number[] joinNumbers = [5,54.65,67,231]; 
	map[integer, number] joinMap = [5,54.65,67,231]; 
	joinMap[80] = 5455.987; 
	joinMap[-5] = 5455.987; 
	joinMap[3] = 0.1; 
	
	joinedString1 = join('♫', joinMap);
	joinedString3 = join('"', joinNumbers);
	joinedString3 = join('☺', joinNumbers, joinMap, "CTL2", 42);
	
	return 0;
}