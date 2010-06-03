string input;
string trim1;

function integer transform() {
	input='The QUICk !!$  broWn fox juMPS over the lazy DOG  	 ';
	trim1=trim('	  im  '+input);
	printErr('after trim:'+trim1 );
	return 0;
}