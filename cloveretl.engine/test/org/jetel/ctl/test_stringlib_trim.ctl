string input;
string trim1;

string trim_empty;
string trim_null;

function integer transform() {
	input='The QUICk !!$  broWn fox juMPS over the lazy DOG  	 ';
	trim1=trim('	  im  '+input);
	printErr('after trim:'+trim1 );
	trim_empty = trim('');
//	trim_null = trim(null);
	return 0;
}