string input;
integer index;
integer index1;
integer index2;
integer index3;
integer index4;

integer index5;
integer index6;
integer index7;
integer index8;
integer index9;
integer index10;

integer index_empty1;
integer index_empty2;
integer index_empty3;
integer index_empty4;

function integer transform() {
	input='hello world';
	printErr(input); 
	
	index=indexOf(input,'l');
	printErr('index of l: ' + index); 
	
	index5 = indexOf(input, 'k');
	printErr('index5: [' +index5+']' );
	
	index6 = indexOf(input, ''); 
	printErr('index6: [' +index6+']' );
	
	index7 = indexOf('hello world','', 4);
	printErr('index7: [' +index7+']' );
	
	index8 = indexOf("milk",'',15);
	
	index1=indexOf(input,'l',5);
	printErr('index of l since 5: ' + index1);
	
	index2=indexOf(input,'hello');
	printErr('index of hello: ' + index2);
	
	index3=indexOf(input,'hello',1);
	printErr('index of hello since 1: ' + index3);
	
	index4=indexOf(input,'world',1);
	printErr('index of world: ' + index4);
	
	index9 = indexOf('aaab','a',13);
	printErr('index9: ' + index9);
	
	index10 = indexOf('aaab','',2);
	printErr('index10: '+index10);
	
	index_empty1 = indexOf('','a');
	printErr('index_empty1: [' +index_empty1+']' );
	
	index_empty2 = indexOf('','');
	printErr('index_empty2: [' +index_empty2+']' );
	
	index_empty3 = indexOf('','', 12);
	printErr('index_empty3: [' +index_empty3+']' );
	
	index_empty4 = indexOf('','a',3);
	printErr('index_empty4: [' +index_empty4+']' );
	
	return 0;
}