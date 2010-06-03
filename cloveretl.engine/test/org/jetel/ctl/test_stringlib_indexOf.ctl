string input;
integer index;
integer index1;
integer index2;
integer index3;
integer index4;

function integer transform() {
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