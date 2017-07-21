string[] sourceFields_nullTarget;

string[] sourceFields1;
string[] sourceFields2;
string[] sourceFields3;
string[] sourceFields4;
string[] sourceFields5;
string[] sourceFields6;

string[] targetFields_nullSource;

string[] targetFields;
string[] targetFields1;
string[] targetFields2;
string[] targetFields3;

boolean sourceMapped_nullTarget;
boolean targetMapped_nullSource;

boolean isSourceMapped1;
boolean isSourceMapped2;
boolean isSourceMapped3;
boolean isSourceMapped4;
boolean isTargetMapped;

string mappingCode = "$name=$name;$name=$firstName;$countryName=$countryName;#$phone=$field1;";
string fieldName = "name";
integer indexNum = 0;

function integer transform() {
	// white space allowed among separators
	getMappedSourceFields("  #$phone  = $field1  ;  ", "name ", 1);
	// field name can be null
	sourceFields_nullTarget = getMappedSourceFields("$name=$name;$name=$firstName;$countryName=$countryName;#$phone=$field1;", null, 1); 
	getMappedSourceFields(";|:##:##", "name", 4);
	getMappedSourceFields(mappingCode, "name", indexNum);
	getMappedSourceFields(mappingCode, fieldName, 0);
	

	sourceFields1 = getMappedSourceFields("$name=$name;$name=$firstName;$countryName=$countryName;#$phone=$field1;", "name", 0);
	sourceFields2 = getMappedSourceFields("$name=$name;$name=$firstName;$countryName=$countryName;#$phone=$field1;", "name", 1);
	sourceFields3 = getMappedSourceFields("##$name(a)=$a;$name(d)=$b;$name=$c;$c(a)=$d;#$phone=$field1;", "name", 2); 
	sourceFields4 = getMappedSourceFields(mappingCode, fieldName, indexNum);
	sourceFields5 = getMappedSourceFields("$name=$name;$name=$firstName;#$name=$countryName;#$name=$countryName;", "name");
	sourceFields6 = getMappedSourceFields("$name=$name;$name=$firstName;#$name=$countryName;#$name=$countryName;");
	
	targetFields_nullSource = getMappedSourceFields("$name=$name;$name=$firstName;$countryName=$countryName;#$phone=$field1;", null, 1); 

	targetFields = getMappedTargetFields("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "name", 0);
	targetFields1 = getMappedTargetFields("$name=$input;$name=$input;#$name=$input;#$name=$input;", "input");
	targetFields2 = getMappedTargetFields("$field1=$name;$field2=$name;$field1=$name;$field1=$name;#$field3=$name;", "name", 0);
	targetFields3 = getMappedTargetFields("$field1=$name;$field2=$name;$field1=$name;$field1=$name;#$field3=$name;");
	
	isSourceMapped1 = isSourceFieldMapped("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "name", 0);
	isSourceMapped2 = isSourceFieldMapped("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "name", 1);
	isSourceMapped3 = isSourceFieldMapped("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "field1");
	
	isTargetMapped = isTargetFieldMapped("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", "name");
	
	sourceMapped_nullTarget = isSourceFieldMapped("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", null, 0);
	targetMapped_nullSource = isTargetFieldMapped("$name=$name;$name=$firstName;$countryName=$name;#$phone=$field1;", null);
	
	return 0;
}