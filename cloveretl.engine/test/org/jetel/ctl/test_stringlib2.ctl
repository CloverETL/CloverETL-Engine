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
	test=remove_diacritic('teścik');
	print_err(test); 
	
	test1=remove_diacritic('žabička');
	print_err(test1);
	
	t=translate('hello','leo','pii');
	print_err(t); 
	
	t1=translate('hello','leo','pi');
	print_err(t1); 
	
	t2=translate('hello','leo','piims');
	print_err(t2); 
	
	t3=translate('hello','hleo','');
	print_err(t3); 
	
	t4=translate('my language needs the letter e', 'egms', 'X');
	print_err(t4); 
	
	input='hello world';
	print_err(input); 
	
	index=index_of(input,'l');
	print_err('index of l: ' + index); 
	
	index1=index_of(input,'l',5);
	print_err('index of l since 5: ' + index1);
	
	index2=index_of(input,'hello');
	print_err('index of hello: ' + index2);
	
	index3=index_of(input,'hello',1);
	print_err('index of hello since 1: ' + index3);
	
	index4=index_of(input,'world',1);
	print_err('index of world: ' + index4);
	return 0;
}