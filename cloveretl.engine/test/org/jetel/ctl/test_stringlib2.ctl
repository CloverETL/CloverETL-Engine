string test;
string test1;
string t;
string t1;
string t2;
string t3;
string t4;
string input;
integer index;
integer index1;
integer index2;
integer index3;
integer index4;

function integer transform() {
	test=removeDiacritic('teścik');
	printErr(test); 
	
	test1=removeDiacritic('žabička');
	printErr(test1);
	
	t=translate('hello','leo','pii');
	printErr(t); 
	
	t1=translate('hello','leo','pi');
	printErr(t1); 
	
	t2=translate('hello','leo','piims');
	printErr(t2); 
	
	t3=translate('hello','hleo','');
	printErr(t3); 
	
	t4=translate('my language needs the letter e', 'egms', 'X');
	printErr(t4); 
	
	input='hello world';
	printErr(input); 
	
	index=indexOf(input,'l');
	printErr('index of l: ' + index); 
	
	index1=indexOf(input,'l',5);
	printErr('index of l since 5: ' + index1);
	
	index2=indexOf(input,'hello');
	printErr('index of hello: ' + index2);
	
	index3=indexOf(input,'hello',1);
	printErr('index of hello since 1: ' + index3);
	
	index4=indexOf(input,'world',1);
	printErr('index of world: ' + index4);
	return 0;
}