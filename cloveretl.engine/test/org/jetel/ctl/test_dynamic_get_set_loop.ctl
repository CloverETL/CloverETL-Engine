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
	setIntValue($out.0, valueIndex, 654321);
	value[0] = $out.0.Value;

	// dynamic access using field name
	setIntValue($out.1, valueName, 777777);
	value[1] = $out.1.Value;

	value[2] = $out.0.Value++; // post-increment
	value[3] = ++$out.0.Value; // pre-increment
	
	// test dynamic access to a variable of type record
	setIntValue(varInput, valueIndex, 123456);
	value[4] = varInput.Value;

	setIntValue($out.0.*, valueIndex, 112567); // $out.0.*
	value[5] = $out.firstOutput.Value; // $out.firstOutput
	
	$out.0.*.setIntValue(valueIndex, 112233);
	value[6] = $out.0.*.getIntValue(valueName);
	
	recordLength = length($out.0);
	for(integer i = 0; i < recordLength; i++) {
		type[i] = getFieldType($out.0, i);
		integer tmpVar = 1000 + i;
		switch(type[i]) {
			case "boolean":
				setBoolValue($out.0, i, (tmpVar % 2) == 0);
				break;
			case "date":
				setDateValue($out.0, i, long2date(tmpVar));
				break;
			case "decimal":
				setDecimalValue($out.0, i, tmpVar);
				break;
			case "integer":
				setIntValue($out.0, i, tmpVar);
				break;
			case "long":
				setLongValue($out.0, i, tmpVar);
				break;
			case "number":
				setNumValue($out.0, i, tmpVar);
				break;
			case "string":
				setStringValue($out.0, i, num2str(tmpVar));
				break;
		}
		asString[i] = getValueAsString($out.0, i);
		isNull[i] = isNull($out.0, i);
		fieldName[i] = getFieldName($out.0, i);
		fieldIndex[i] = getFieldIndex($out.0, fieldName[i]);
	}
	
	for(integer i = 0; i < length($out.0); i++) {
		string fieldType = getFieldType($out.0, i);
		switch(fieldType) {
			case "boolean":
				setBoolValue($out.0, i, true);
				booleanVar = getBoolValue($out.0, i);
				break;
			case "byte":
				setByteValue($out.0, i, hex2byte("1234567890abcdef"));
				byteVar = getByteValue($out.0, i);
				break;
			case "date":
				setDateValue($out.0, i, long2date(5000));
				dateVar = getDateValue($out.0, i);
				break;
			case "decimal":
				setDecimalValue($out.0, i, 1000.125D);
				decimalVar = getDecimalValue($out.0, i);
				break;
			case "integer":
				setIntValue($out.0, i, 1000);
				integerVar = getIntValue($out.0, i);
				break;
			case "long":
				setLongValue($out.0, i, 1000000000000L);
				longVar = getLongValue($out.0, i);
				break;
			case "number":
				setNumValue($out.0, i, 1000.5);
				numberVar = getNumValue($out.0, i);
				break;
			case "string":
				setStringValue($out.0, i, "hello");
				stringVar = getStringValue($out.0, i);
				break;
		}
	}

	return ALL;
}
