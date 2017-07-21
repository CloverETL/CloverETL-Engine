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

boolean[] someValue;
boolean[] nullValue;
string[] asString2;
boolean[] isNull2;

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

	firstInput nullRecord;
	for(integer i = 0; i < length(nullRecord); i++) {
		type[i] = getFieldType(nullRecord, i);
		switch(type[i]) {
			case "boolean":
				setBoolValue(nullRecord, i, true);
				//assertEquals(true, getBoolValue(nullRecord, i));
				someValue[i] = isnull(getBoolValue(nullRecord, i));
				setBoolValue(nullRecord, i, null);
				//assertEquals(null, getBoolValue(nullRecord, i));
				nullValue[i] = isnull(getBoolValue(nullRecord, i));
				break;
			case "byte":
				setByteValue(nullRecord, i, hex2byte("1234567890abcdef"));
				//assertEquals(hex2byte("1234567890abcdef"), getByteValue(nullRecord, i));
				someValue[i] = isnull(getByteValue(nullRecord, i));
				setByteValue(nullRecord, i, null);
				//assertEquals(null, getByteValue(nullRecord, i));
				nullValue[i] = isnull(getByteValue(nullRecord, i));
				break;
			case "date":
				setDateValue(nullRecord, i, 2000-05-05);
				//assertEquals(2000-05-05, getDateValue(nullRecord, i));
				someValue[i] = isnull(getDateValue(nullRecord, i));
				setDateValue(nullRecord, i, null);
				//assertEquals(null, getDateValue(nullRecord, i));
				nullValue[i] = isnull(getDateValue(nullRecord, i));
				break;
			case "decimal":
				setDecimalValue(nullRecord, i, 2);
				//assertEquals(2, getDecimalValue(nullRecord, i));
				someValue[i] = isnull(getDecimalValue(nullRecord, i));
				setDecimalValue(nullRecord, i, null);
				//assertEquals(null, getDecimalValue(nullRecord, i));
				nullValue[i] = isnull(getDecimalValue(nullRecord, i));
				break;
			case "integer":
				setIntValue(nullRecord, i, 3);
				//assertEquals(3, getIntValue(nullRecord, i));
				someValue[i] = isnull(getIntValue(nullRecord, i));
				setIntValue(nullRecord, i, null);
				//assertEquals(null, getIntValue(nullRecord, i));
				nullValue[i] = isnull(getIntValue(nullRecord, i));
				break;
			case "long":
				setLongValue(nullRecord, i, 4);
				//assertEquals(4, getLongValue(nullRecord, i));
				someValue[i] = isnull(getLongValue(nullRecord, i));
				setLongValue(nullRecord, i, null);
				//assertEquals(null, getLongValue(nullRecord, i));
				nullValue[i] = isnull(getLongValue(nullRecord, i));
				break;
			case "number":
				setNumValue(nullRecord, i, 5);
				//assertEquals(5, getNumValue(nullRecord, i), 0.1);
				someValue[i] = isnull(getNumValue(nullRecord, i));
				setNumValue(nullRecord, i, null);
				//assertEquals(null, getNumValue(nullRecord, i), 0.1);
				nullValue[i] = isnull(getNumValue(nullRecord, i));
				break;
			case "string":
				setStringValue(nullRecord, i, "foo");
				//assertEquals("foo", getStringValue(nullRecord, i));
				someValue[i] = isnull(getStringValue(nullRecord, i));
				setStringValue(nullRecord, i, null);
				//assertEquals(null, getStringValue(nullRecord, i));
				nullValue[i] = isnull(getStringValue(nullRecord, i));
				break;
		}
		//assertEquals(null, getValueAsString(nullRecord, i));
		asString2[i] = getValueAsString(nullRecord, i);
		//assertEquals(true, isNull(nullRecord, i));
		isNull2[i] = isNull(nullRecord, i);
	}

	return ALL;
}
