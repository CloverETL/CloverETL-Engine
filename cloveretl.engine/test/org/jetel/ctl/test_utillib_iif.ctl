string ret1;
date ret2;
byte ret3;
integer ret4;
long ret5;
number ret6;
decimal ret7;
boolean ret8;

function integer transform(){
	boolean b1 = true;
	boolean b2 = false;
	
	ret1 = iif(b1, 'Renektor', null);
	ret2 = iif(b2, str2date('2006-11-12','yyyy-MM-dd'),str2date('2005-11-12','yyyy-MM-dd'));
	ret3 = iif(b1, str2byte('Akali', 'UTF-8'), str2byte('Darius', 'UTF-8'));
	ret4 = iif(b2, 154, 236);
	ret5 = iif(b1, 78L, 97l);
	ret6 = iif(b2, 15.98, 78.2);
	ret7 = iif(b1, 87.69d, 578.32d);
	ret8 = iif(b2, b2, b1);
	
	return 0;
}