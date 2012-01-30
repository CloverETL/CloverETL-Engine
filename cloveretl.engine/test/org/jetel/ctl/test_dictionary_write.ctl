string assignmentReturnValue;

function integer transform() {

	assignmentReturnValue = (dictionary.s = "Guil");
	dictionary.i = 831 + 1;
	dictionary.l = dshift( 54 ); 
	dictionary.d = dictionary.d621;
	dictionary.n = 934.2;
	dictionary.a = str2date('02.Dec/1992','dd.MMM/yyyy','en.US');
	dictionary.b = true;
	dictionary.y = hex2byte("12A2");
	dictionary.stringList = ["xx", null];
	dictionary.dateList = [long2date(98000), null, long2date(76000)];
	dictionary.byteList = [null, hex2byte("ABCD"), hex2byte("EF")];

	return 0;
}

function long dshift(long l) {
	return l*10;
}