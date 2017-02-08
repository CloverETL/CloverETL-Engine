import "${PROJECT}/import_CLO-10313_master.ctl";

string str;

// Transforms input record into output record.
function integer transform() {
	str = getIntAsStr();

	return ALL;
}
