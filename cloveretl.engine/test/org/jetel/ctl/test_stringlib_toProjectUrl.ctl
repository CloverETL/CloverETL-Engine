string ret1;
string ret2;
string ret3;
string ret4;
string ret5;
string ret6;

function integer transform(){
	ret1 = toProjectUrl('/file.txt');
	ret2 = toProjectUrl('');
	ret3 = toProjectUrl('ftp://kennen_ftp/home');
	ret4 = toProjectUrl(null);
	ret5 = toProjectUrl("file:/home/user/workspace/myproject");
	ret6 = toProjectUrl("sandbox://mysandbox/user/workspace/myproject");
	return 0;
}