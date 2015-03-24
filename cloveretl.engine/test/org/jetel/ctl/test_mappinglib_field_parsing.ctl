string[] sourceFields1;
string[] sourceFields2;
string[] sourceFields3;

string[] targetFields;

boolean isSourceMapped1;
boolean isSourceMapped2;
boolean isTargetMapped;

function integer transform() {
	sourceFields1 = getMappedSourceFields("$name=$name;$name=$firstName;$countryName=$countryName;#$phone=$field1;", "name", 0);
	sourceFields2 = getMappedSourceFields("$name=$name;$name=$firstName;$countryName=$countryName;#$phone=$field1;", "name", 1);
	// white space allowed among separators
	getMappedSourceFields("  #$phone  = $field1  ;  ", "name ", 1);
	// field name can be null
	getMappedSourceFields("$name=$name;$name=$firstName;$countryName=$countryName;#$phone=$field1;", null, 1); 
	getMappedSourceFields(";|:##:##", "name", 4);
	sourceFields3 = getMappedSourceFields("##$name(a)=$a;$name(d)=$b;$name=$c;$c(a)=$d;#$phone=$field1;", "name", 2); 
	
	targetFields = getMappedTargetFields("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "name", 0);
	
	isSourceMapped1 = isSourceFieldMapped("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "name", 0);
	isSourceMapped2 = isSourceFieldMapped("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "name", 1);
	
	isTargetMapped = isTargetFieldMapped("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "name");
	
	return 0;
}