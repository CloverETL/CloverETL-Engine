function void doSomething() {
	
	if (true) {
		printLog(info, "Might crash now.");
		return;
	}

	// unreachable code	
	raiseError("Should never get here.");
}

function integer transform() {

	doSomething();

	printLog(info, "Did not crash.");

	return ALL;
}
