integer a;
integer[] haystack;
integer needle;
boolean b1;
boolean b2;
double[] h2;
boolean b3;
string[] h3;
string n3; 
boolean b4;
boolean ret1;
boolean ret2;
boolean ret3;
boolean ret4;
boolean ret5;
boolean ret6;
boolean ret7;
boolean ret8;
boolean ret9;
boolean ret10;
boolean ret11;
boolean ret12;
boolean ret13;
boolean ret14;
boolean ret15;
boolean ret16;
boolean ret17;
boolean ret18;
boolean ret19;

function integer transform() {
	a = 1;
	haystack = [a,a+1,a+1+1];
	needle = 2;
	b1 = needle.in(haystack);
	printErr(needle + ' in ' + haystack + ': ' + b1);
	haystack.clear();
	b2 = in(needle,haystack);
	printErr(needle + ' in ' + haystack + ': ' + b2);
	h2 = [ 2.1, 2.0, 2.2];
	b3 = needle.in(h2);
	printErr(needle + ' in ' + h2 + ': ' + b3);
	h3 = [ 'memento', 'mori', 'memento ' + 'mori'];
	n3 = 'memento ' + 'mori'; 
	b4 = n3.in(h3);
	printErr(n3 + ' in ' + h3 + ': ' + b4);
	
	string str = null;
	ret1 = str.in(['bb','cc']);
	ret2 = str.in([null, 'aa']);
	
	byte b = null;
	ret3 = b.in([str2byte('Anivia', 'utf-8'), str2byte('Le Blanc', 'utf-8')]);
	ret4 = in(b, [null, str2byte('Ahri', 'utf-8')]);
	
	date datum = null;
	ret5 = in(datum, [2012-2-14, 2010-6-24]);
	ret6 = in(datum, [2012-2-14, null]);
	
	integer int = null;
	ret7 = int.in([12,15]);
	ret8 = int.in([12,null,15]);
	
	long l = null;
	ret9 = in(l, [34l, 902l]);
	ret10 = l.in([15l, null]);
	
	decimal dec = null;
	ret11 = in(dec, [12.5d, 45.6d]);
	ret12 = dec.in([null, null, 78.9d]);
	
	number num = null;
	ret13 = num.in([78, 12.6]);
	ret14 = in(num, [null, 89.7]);
	
	long[] lList;
	ret15 = in(l,lList);
	ret16 = in(l, [null,null,null]);
	
	map[long, string] myMap;
	ret17 = in(l, myMap);;
	myMap[1L] = 'Warwick';
	ret18 = in(l, myMap);
	myMap[l] = 'Yorick';
	ret19 = in(l, myMap);
	return 0;
}