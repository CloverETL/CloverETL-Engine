integer ret1;
integer ret2;
integer ret3;

function integer transform(){
	ret1 = getFieldIndex($in.0, 'Age');
	ret2 = $in.0.getFieldIndex('Age');
	ret3 = getFieldIndex($in.0, 'Rengar');
	return 0;
}