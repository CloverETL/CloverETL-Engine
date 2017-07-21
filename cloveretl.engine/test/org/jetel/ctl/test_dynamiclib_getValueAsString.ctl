string ret1;
string ret2;
string ret3;
string ret4;
string ret5;
string ret6;
string ret7;
string ret8;
string ret9;
string ret10;

function integer transform(){
	ret1 = getValueAsString($in.0, 0);
	ret2 = $in.0.getValueAsString('Age');
	ret3 = getValueAsString($in.0 ,'City');
	ret4 = $in.0.getValueAsString(3);
	ret5 = getValueAsString($in.0, 4);
	ret6 = $in.0.getValueAsString('Value');
	ret7 = getValueAsString($in.0, 'Flag');
	ret8= $in.0.getValueAsString(7);
	ret9= getValueAsString($in.0, 8);
	$out.0.City = null;
	ret10 = getValueAsString($out.0, 'City');
	
	printErr(ret1);
	printErr(ret2);
	printErr(ret3);
	printErr(ret4);
	printErr(ret5);
	printErr(ret6);
	printErr(ret7);
	printErr(ret8);
	printErr(ret9);
	printErr(ret10);
	return 0;
}