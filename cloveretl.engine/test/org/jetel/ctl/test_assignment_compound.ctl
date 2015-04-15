// copied from ctl2-language-reference.xml

integer i = 5;
i += 4; // i == 9

integer ni = null;
ni += 5; // ni == 5

string s = "hello ";
s += "world "; // s == "hello world "
s += 123; // s == "hello world 123"		

string ns = null;
ns += "hello"; // ns == "hello"

string ns2 = null;
ns2 = ns2 + "hello"; // ns2 == "nullhello" 

integer[] list1 = [1, 2, 3];
integer[] list2 = [4, 5];
list1 += list2; // list1 == [1, 2, 3, 4, 5]

map[string, integer] map1;
map1["1"] = 1;
map1["2"] = 2;
map[string, integer] map2;
map2["2"] = 22;
map2["3"] = 3;
map1 += map2; // map1: "1"->1, "2"->22, "3"->3

long l = 10L;
l -= 4; // l == 6L;

decimal d = 12.34D;
d *= 2; // d == 24.68D;

number n = 6.15;
n /= 1.5; // n ~ 4.1

long r = 27;
r %= 10; // r == 7L

function integer transform() {

	return 0;
}