integer recordLength;

integer[] value;
decimal[] currency;
string[] type;
string[] asString;
boolean[] isNull;
string[] fieldName;
integer[] fieldIndex;

boolean booleanVar;
byte byteVar;
date dateVar;
decimal decimalVar;
integer integerVar;
long longVar;
number numberVar;
string stringVar;

// Transforms input record into output record.
function integer transform() {

	firstInput varInput;

	integer valueIndex = 5;
	string valueName = "Value";
	integer currencyIndex = 8;
	string currencyName = "Currency";
	
	// dynamic access using field index
	$out.0.setIntValue(valueIndex, 654321);
	value[0] = $out.0.Value;

	// dynamic access using field name
	$out.1.setIntValue(valueName, 777777);
	value[1] = $out.1.Value;

	value[2] = $out.0.Value++; // post-increment
	value[3] = ++$out.0.Value; // pre-increment
	
	// test dynamic access to a variable of type record
	varInput.setIntValue(valueIndex, 123456);
	value[4] = varInput.Value;

	$out.0.*.setIntValue(valueIndex, 112567); // $out.0.*
	value[5] = $out.firstOutput.Value; // $out.firstOutput
	
	$out.0.*.setIntValue(valueIndex, 112233);
	value[6] = $out.0.*.getIntValue(valueName);
	
	recordLength = $out.0.length();
	for(integer i = 0; i < recordLength; i++) {
		type[i] = $out.0.getFieldType(i);
		integer tmpVar = 1000 + i;
		switch(type[i]) {
			case "boolean":
				$out.0.setBoolValue(i, (tmpVar % 2) == 0);
				break;
			case "date":
				$out.0.setDateValue(i, long2date(tmpVar));
				break;
			case "decimal":
				$out.0.setDecimalValue(i, tmpVar);
				break;
			case "integer":
				$out.0.setIntValue(i, tmpVar);
				break;
			case "long":
				$out.0.setLongValue(i, tmpVar);
				break;
			case "number":
				$out.0.setNumValue(i, tmpVar);
				break;
			case "string":
				$out.0.setStringValue(i, num2str(tmpVar));
				break;
		}
		asString[i] = $out.0.getValueAsString(i);
		isNull[i] = $out.0.isNull(i);
		fieldName[i] = $out.0.getFieldName(i);
		fieldIndex[i] = $out.0.getFieldIndex(fieldName[i]);
	}
	
	for(integer i = 0; i < $out.0.length(); i++) {
		string fieldType = $out.0.getFieldType(i);
		switch(fieldType) {
			case "boolean":
				$out.0.setBoolValue(i, true);
				booleanVar = $out.0.getBoolValue(i);
				break;
			case "byte":
				$out.0.setByteValue(i, hex2byte("1234567890abcdef"));
				byteVar = $out.0.getByteValue(i);
				break;
			case "date":
				$out.0.setDateValue(i, long2date(5000));
				dateVar = $out.0.getDateValue(i);
				break;
			case "decimal":
				$out.0.setDecimalValue(i, 1000.125D);
				decimalVar = $out.0.getDecimalValue(i);
				break;
			case "integer":
				$out.0.setIntValue(i, 1000);
				integerVar = $out.0.getIntValue(i);
				break;
			case "long":
				$out.0.setLongValue(i, 1000000000000L);
				longVar = $out.0.getLongValue(i);
				break;
			case "number":
				$out.0.setNumValue(i, 1000.5);
				numberVar = $out.0.getNumValue(i);
				break;
			case "string":
				$out.0.setStringValue(i, "hello");
				stringVar = $out.0.getStringValue(i);
				break;
		}
	}

	return ALL;
}
