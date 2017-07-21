string[] copyStrList;
string[] retStrList;
string[] copyStrList2;
string[] retStrList2;

date[] copyDateList;
date[] retDateList;
date[] copyDateList2;
date[] retDateList2;

integer[] copyIntList;
integer[] retIntList;
integer[] copyIntList2;
integer[] retIntList2;

long[] copyLongList;
long[] retLongList;
long[] copyLongList2;
long[] retLongList2;

number[] copyNumList;
number[] retNumList;
number[] copyNumList2;
number[] retNumList2;

decimal[] copyDecList;
decimal[] retDecList;
decimal[] copyDecList2;
decimal[] retDecList2;

integer[] copyEmpty;
integer[] retEmpty;
integer[] copyEmpty2;
integer[] retEmpty2;
integer[] copyEmpty3;
integer[] retEmpty3;

function integer transform() {
	copyStrList = ['Elise','Volibear','Jarvan IV'];
	retStrList = insert(copyStrList,2,'Garen');
	copyStrList2 = ['Jax','Ashe'];
	string[] names = ['Aatrox', 'Lisandra'];
	retStrList2 = insert(copyStrList2,1,names);
	
	copyDateList = [str2date('2009-11-12','yyyy-MM-dd')];
	retDateList = insert(copyDateList,0,str2date('2008-03-07','yyyy-MM-dd'));
	copyDateList2 =[str2date('2003-02-01','yyyy-MM-dd')];
	retDateList2 = insert(copyDateList2,1,retDateList);
	
	copyIntList = [1,2,3,4,5,6,7];
	retIntList = insert(copyIntList,3,12);
	integer[] add = [12,13]; 
	copyIntList2 = [10,11];
	retIntList2 = insert(copyIntList2,1,add);
	
	copyLongList = [15L,16L,17L];
	retLongList = insert(copyLongList,0,14L);
	long[] arr = [22L,23L];
	copyLongList2 = [20L,21L];
	retLongList2 = insert(copyLongList2, 2, arr);
	
	copyNumList = [12.3,15.4];
	retNumList = insert(copyNumList,1,11.1);
	number[] nums = [44.4,55.5];
	copyNumList2 = [22.2,33.3];
	retNumList2 = insert(copyNumList2,1,nums);

	copyDecList = [11.1d,22.2d];
	retDecList = insert(copyDecList,2,33.3d);
	copyDecList2 = [1.1d,2.2d];
	decimal[] decs = [3.3d,4.4d];
	retDecList2 = insert(copyDecList2,0,decs);
	
	retEmpty = insert(copyEmpty,0,11);
	retEmpty2 = insert(copyEmpty2,0,add);
	integer[] tmp_arr;
	retEmpty3 = insert(copyEmpty3,0,tmp_arr);
	
	return 0;
}