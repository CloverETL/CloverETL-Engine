string[] sourceFields1;
string[] sourceFields2;

string[] targetFields;

boolean isSourceMapped1;
boolean isSourceMapped2;
boolean isTargetMapped;

function integer transform() {
	// assign null values from third input record to first output record
	sourceFields1 = getMappedSourceFields("$name=$name;$name=$firstName;$countryName=$countryName;#$phone=$field1;", "name", 0);
	sourceFields2 = getMappedSourceFields("$name=$name;$name=$firstName;$countryName=$countryName;#$phone=$field1;", "name", 1);
	
	targetFields = getMappedTargetFields("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "name", 0);
	
	isSourceMapped1 = isSourceFieldMapped("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "name", 0);
	isSourceMapped2 = isSourceFieldMapped("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "name", 1);
	
	isTargetMapped = isTargetFieldMapped("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "name");
	
	return 0;
}