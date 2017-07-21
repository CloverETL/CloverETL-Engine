string trans;
string trans1;
string trans2;
string trans3;
string trans4;

function integer transform() {
	trans=translate('hello','leo','pii');
	printErr(trans); 
	
	trans1=translate('hello','leo','pi');
	printErr(trans1); 
	
	trans2=translate('hello','leo','piims');
	printErr(trans2); 
	
	trans3=translate('hello','hleo','');
	printErr(trans3); 
	
	trans4=translate('my language needs the letter e', 'egms', 'X');
	printErr(trans4); 
	
	return 0;
}