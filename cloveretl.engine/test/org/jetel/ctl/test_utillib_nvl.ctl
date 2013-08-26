string ret1;
string ret2;
byte ret3;
byte ret4;
date ret5;
date ret6;
integer ret7;
integer ret8;
long ret9;
long ret10;
number ret11;
number ret12;
decimal ret13;
decimal ret14;
string ret15;

function integer transform(){
	ret1 = nvl(null, 'Fiora');
	ret2 = nvl('Olaf', 'Evelynn');
	ret3 = nvl(null, str2byte('Elise','utf-8'));
	ret4 = nvl(str2byte('Diana','utf-8'),str2byte('Fizz', 'utf-8'));
	ret5 = nvl(null, str2date('2005-05-13','yyyy-MM-dd'));
	ret6 = nvl(str2date('2004-03-14','yyyy-MM-dd'),str2date('2001-11-13','yyyy-MM-dd'));
	ret7 = nvl(null,7);
	ret8 = nvl(8,9);
	ret9 = nvl(null, 111l);
	ret10 = nvl(112l, 133l);
	ret11 = nvl(null, 10.1);
	ret12 = nvl(10.2, 10.3);
	ret13 = nvl(null, 12.2d);
	ret14 = nvl(12.3d,213.66d);
	ret15 = nvl(null,null);
	return 0;
	
}