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

string[] example_getMappedSourceFields1;
string[] example_getMappedSourceFields2;
string[] example_getMappedSourceFields3;
string[] example_getMappedTargetFields1;
string[] example_getMappedTargetFields2;
string[] example_getMappedTargetFields3;
boolean example_isSourceFieldMapped1;
boolean example_isSourceFieldMapped2;
boolean example_isSourceFieldMapped3;
boolean example_isSourceFieldMapped4;
boolean example_isTargetFieldMapped1;
boolean example_isTargetFieldMapped2;

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

	// examples from the documentation:
	example_getMappedSourceFields1 = getMappedSourceFields("$target1=$srcA1;$target3=$srcA1;#$target1=$srcB4;$target2=$srcB2;", "target1", 1);
	example_getMappedSourceFields2 = getMappedSourceFields("$target1=$source1;$target1=$source3;$target3=$source4;", "target1");
	example_getMappedSourceFields3 = getMappedSourceFields("$target1=$source1;$target1=$source3;$target3=$source4;");
	
	example_getMappedTargetFields1 = getMappedTargetFields("$target1=$src1;$target3=$src1;#$target1=$src4;$target2=$src1;", "src1", 1);
	example_getMappedTargetFields2 = getMappedTargetFields("$target1=$src1;$target3=$src1;#$target1=$src4;$target2=$src1;", "src1");
	example_getMappedTargetFields3 = getMappedTargetFields("$target1=$source1;$target1=$source3;$target3=$source4;") ;
	
	example_isSourceFieldMapped1 = isSourceFieldMapped("$target1=$srcA1;$target3=$srcA1;#$target1=$srcB4;$target2=$srcB2;", "srcB2", 1);
	example_isSourceFieldMapped2 = isSourceFieldMapped("$target1=$srcA1;$target3=$srcA1;#$target1=$srcB4;$target2=$srcB2;", "srcB2", 0);
	example_isSourceFieldMapped3 = isSourceFieldMapped("$target1=$source1;$target1=$source3;$target3=$source4;", "source3");
	example_isSourceFieldMapped4 = isSourceFieldMapped("$target1=$source1;$target1=$source3;$target3=$source4;", "source2");
	
	example_isTargetFieldMapped1 = isTargetFieldMapped("$target1=$srcA1;$target3=$srcA1;#$target1=$srcB4;$target2=$srcB2;", "target2");
	example_isTargetFieldMapped2 = isTargetFieldMapped("$target1=$source1;$target1=$source3;$target3=$source4;", "target2");
	
	return 0;
}