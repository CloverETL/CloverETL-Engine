string input;
string[] findList;
string[] findList2;
string[] findList3;
string[] findList4; 
string[] findList5;
string[] findList6;

function integer transform() {
	input='The quick brown fox jumps over the lazy dog';
	findList = find(input, '[^o]+');
	findList2 = find('mark.twain@javlin.eu','^[a-z]*.[a-z]*');
	findList3 = find('','^[a-z]*.[a-z]*');
	findList4 = find('mark','');
	findList5 = find('mark.twain@javlin.eu','(^[a-z]*).([a-z]*)',2);	
	findList6 = find('','');
	printErr(findList6);
	return 0;
}