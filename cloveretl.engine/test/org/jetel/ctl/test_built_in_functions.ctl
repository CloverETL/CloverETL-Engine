integer notNullValue;
integer nullValue;
boolean isNullRes1;
boolean isNullRes2;
integer nvlRes1;
integer nvlRes2;
integer nvl2Res1;
integer nvl2Res2;
integer iifRes1;
integer iifRes2;

function integer transform() {
	notNullValue = 1;
	nullValue = null;
	isNullRes1 = isnull(notNullValue);
	isNullRes2 = isnull(nullValue);
	nvlRes1 = nvl(notNullValue,2);
	nvlRes2 = nvl(nullValue,2);
	nvl2Res1 = nvl2(notNullValue,1,2);
	nvl2Res2 = nvl2(nullValue,1,2);
	iifRes1 = iif(isnull(notNullValue),1,2);
	iifRes2 = iif(isnull(nullValue),1,2);
	print_err('This message belongs to standard error');
	print_log(debug, 'This message belongs to DEBUG');
	print_log(info, 'This message belongs to INFO');
	print_log(warn, 'This message belongs to WARN');
	print_log(error, 'This message belongs to ERROR');
	print_log(fatal, 'This message belongs to FATAL');
	print_log(trace, 'This message belongs to TRACE');
	return 0;
}