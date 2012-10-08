function integer transform() {
	matchGroups("abcdef", ".*")[0] = "XXX";

	return 0;
} 