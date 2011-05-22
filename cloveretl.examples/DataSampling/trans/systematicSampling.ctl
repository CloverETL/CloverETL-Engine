//#CTL2
integer counter = 0;
integer rec_no = ${rec_no};//next record to select

// Transforms input record into output record.
function integer transform() {
	counter++;
	if (counter == rec_no) {//current record is record to select
		rec_no = rec_no + double2integer(${timing});//prepare next index to select
	}else{//don't select current record
		return SKIP;
	}
	$0.* = $0.*;
	return ALL;
}
// Called to return a user-defined error message when an error occurs.
// function getMessage() {}

// Called during component initialization.
// function init() {}

// Called after the component finishes.
// function finished() {}
