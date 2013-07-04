string concat;
string concat1;
string concat2;
string concat3;

function integer transform() {
	concat=concat("");
	concat1=concat("ello hi   ", 'ELLO ', "2,today is ", date2str(today(), "yyyy MMM dd"));
	concat2=concat("","");
	concat3=concat("","","clover");
	printErr('concatenation:' + concat1);
	return 0;
}