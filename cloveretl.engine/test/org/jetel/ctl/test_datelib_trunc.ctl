double inputDouble;
date inputDate;
long truncLong;
date truncDate;
integer[] truncList;
map[integer,integer] truncMap;

function integer transform() {
	inputDouble=-pow(3,1.2);
	inputDate=2004-01-02 17:13:20;
	truncList=[1,4,51,57,521,5];
	truncMap[1]=1;
	truncMap[2]=10;
	truncMap[3]=20;
	truncMap[8]=100;
	
	truncLong=trunc(inputDouble);	
	truncDate=trunc(inputDate);
	trunc(truncList);
	trunc(truncMap);
	
	printErr('truncation of '+inputDate+'='+truncDate);
	printErr('truncation of '+inputDouble+'='+truncLong);
	return 0;
}