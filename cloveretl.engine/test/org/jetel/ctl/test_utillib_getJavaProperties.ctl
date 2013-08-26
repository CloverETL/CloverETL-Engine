string java_specification_name;
string my_testing_property;
string ret1;
string ret2;
string ret3;

function integer transform() {
	java_specification_name = getJavaProperties()["java.specification.name"];
	my_testing_property = getJavaProperties()["my.testing.property"];
	getJavaProperties()["my.testing.property2"] = "my value 2";
	ret1 = getJavaProperties()['Karma'];
	string str = null;
//	CLO-1700
//	ret2 = getJavaProperties()[null];
//	ret3 = getJavaProperties()[str];
	return 0;
}