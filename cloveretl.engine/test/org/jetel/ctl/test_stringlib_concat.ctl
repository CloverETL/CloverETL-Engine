string concat;
string concat1;


function integer transform() {
	concat = concat("");
	concat1=concat("ello hi   ", 'ELLO ', "2,today is ", date2str(today(), "yyyy MMM dd"));
	
	printErr('concatenation:' + concat1);
	return 0;
}