string ret1;
string ret2;
string ret3;

function integer transform(){
	ret1 = toAbsolutePath('/file.txt');
	ret2 = toAbsolutePath('');
	ret3 = toAbsolutePath('ftp://kennen_ftp/home');
	return 0;
}