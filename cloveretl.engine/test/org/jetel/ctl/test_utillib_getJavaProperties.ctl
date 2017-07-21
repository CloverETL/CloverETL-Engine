string java_specification_name;
string my_testing_property;

function integer transform() {
	java_specification_name = getJavaProperties()["java.specification.name"];
	my_testing_property = getJavaProperties()["my.testing.property"];
	getJavaProperties()["my.testing.property2"] = "my value 2";
	
	return 0;
}