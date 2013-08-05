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
	
	firstInput nullRecord;
	for(integer i = 0; i < nullRecord.length(); i++) {
		type[i] = nullRecord.getFieldType(i);
		switch(type[i]) {
			case "boolean":
				nullRecord.setBoolValue(i, true);
				//assertEquals(true, nullRecord.getBoolValue(i));
				someValue[i] = isnull(nullRecord.getBoolValue(i));
				nullRecord.setBoolValue(i, null);
				//assertEquals(null, nullRecord.getBoolValue(i));
				nullValue[i] = isnull(nullRecord.getBoolValue(i));
				break;
			case "byte":
				nullRecord.setByteValue(i, hex2byte("1234567890abcdef"));
				//assertEquals(hex2byte("1234567890abcdef"), nullRecord.getByteValue(i));
				someValue[i] = isnull(nullRecord.getByteValue(i));
				nullRecord.setByteValue(i, null);
				//assertEquals(null, nullRecord.getByteValue(i));
				nullValue[i] = isnull(nullRecord.getByteValue(i));
				break;
			case "date":
				nullRecord.setDateValue(i, 2000-05-05);
				//assertEquals(2000-05-05, nullRecord.getDateValue(i));
				someValue[i] = isnull(nullRecord.getDateValue(i));
				nullRecord.setDateValue(i, null);
				//assertEquals(null, nullRecord.getDateValue(i));
				nullValue[i] = isnull(nullRecord.getDateValue(i));
				break;
			case "decimal":
				nullRecord.setDecimalValue(i, 2);
				//assertEquals(2, nullRecord.getDecimalValue(i));
				someValue[i] = isnull(nullRecord.getDecimalValue(i));
				nullRecord.setDecimalValue(i, null);
				//assertEquals(null, nullRecord.getDecimalValue(i));
				nullValue[i] = isnull(nullRecord.getDecimalValue(i));
				break;
			case "integer":
				nullRecord.setIntValue(i, 3);
				//assertEquals(3, nullRecord.getIntValue(i));
				someValue[i] = isnull(nullRecord.getIntValue(i));
				nullRecord.setIntValue(i, null);
				//assertEquals(null, nullRecord.getIntValue(i));
				nullValue[i] = isnull(nullRecord.getIntValue(i));
				break;
			case "long":
				nullRecord.setLongValue(i, 4);
				//assertEquals(4, nullRecord.getLongValue(i));
				someValue[i] = isnull(nullRecord.getLongValue(i));
				nullRecord.setLongValue(i, null);
				//assertEquals(null, nullRecord.getLongValue(i));
				nullValue[i] = isnull(nullRecord.getLongValue(i));
				break;
			case "number":
				nullRecord.setNumValue(i, 5);
				//assertEquals(5, nullRecord.getNumValue(i), 0.1);
				someValue[i] = isnull(nullRecord.getNumValue(i));
				nullRecord.setNumValue(i, null);
				//assertEquals(null, nullRecord.getNumValue(i), 0.1);
				nullValue[i] = isnull(nullRecord.getNumValue(i));
				break;
			case "string":
				nullRecord.setStringValue(i, "foo");
				//assertEquals("foo", nullRecord.getStringValue(i));
				someValue[i] = isnull(nullRecord.getStringValue(i));
				nullRecord.setStringValue(i, null);
				//assertEquals(null, nullRecord.getStringValue(i));
				nullValue[i] = isnull(nullRecord.getStringValue(i));
				break;
		}
		//assertEquals(null, nullRecord.getValueAsString(i));
		asString2[i] = nullRecord.getValueAsString(i);
		//assertEquals(true, nullRecord.isNull(i));
		isNull2[i] = nullRecord.isNull(i);
	}

	return ALL;
}
