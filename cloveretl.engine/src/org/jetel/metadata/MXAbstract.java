package org.jetel.metadata;

import java.util.HashMap;

abstract public class MXAbstract {

	protected static final String NAMESPACE = "";
	protected static final String XMLSCHEMA = "http://www.w3.org/2001/XMLSchema";

	// elements
	protected static final String XSD_ELEMENT = "xsd:element";
	protected static final String XSD_COMPLEX_TYPE = "xsd:complexType";
	protected static final String XSD_SEQUENCE = "xsd:sequence";

	// record attributes
	protected static final String NAME = "name";
	protected static final String TYPE = "type";
	protected static final String FIELD_DELIMITER = "fieldDelimiterStr";
	protected static final String FIELD_NAMES_HEADER = "fieldNamesHeader";
	protected static final String LOCALE_STR = "localeStr";
	protected static final String RECORD_DELIMITER = "recordDelimiterStr";
	protected static final String RECORD_SIZE = "recordSize";
	protected static final String RECORD_SIZE_STRIP_AUTO_FILLING = "recordSizeStripAutoFilling";
	protected static final String REC_TYPE = "recType";
	protected static final String RECORD_PROPERTIES = "recordProperties";
	protected static final String SKIP_FIRST_LINE = "skipFirstLine";
	protected static final String NULLABLE = "nullable";
	protected static final String SPECIFIED_FIELD_DELIMITER = "specifiedFieldDelimiter";
	protected static final String SPECIFIED_RECORD_DELIMITER = "specifiedRecordDelimiter";
	
	// field attributes
	protected static final String AUTO_FILLING = "autoFilling";
	protected static final String DEFAULT_VALUE_STR = "defaultValueStr";
	protected static final String DELIMITER_STR = "delimiterStr";
	protected static final String FORMAT_STR = "formatStr";
	protected static final String DEFAULT_VALUE = "defaultValue";
	protected static final String FIELD_PROPERTIES = "fieldProperties";
	protected static final String SHIFT = "shift";
	protected static final String SIZE = "size";
	
	// properties
	protected static final String PROPERTIES_ASSIGN = "=";
	protected static final String PROPERTIES_DELIM = ";";
	
	// mapping from field type to XSD type representing the field type
	protected static final HashMap<Character, String> typeNames = new HashMap<Character, String>();
	// mapping from field type to XSD type used as base for XSD type representing the field type
	protected static final HashMap<Character, String> primitiveNames = new HashMap<Character, String>();
	// initialize mappings
	static {
		typeNames.put(Character.valueOf(DataFieldMetadata.BYTE_FIELD), "CloverByte");
		typeNames.put(Character.valueOf(DataFieldMetadata.BYTE_FIELD_COMPRESSED), "CloverByteCompressed");
		typeNames.put(Character.valueOf(DataFieldMetadata.DATE_FIELD), "CloverDate");
		typeNames.put(Character.valueOf(DataFieldMetadata.DATETIME_FIELD), "CloverDatetime");
		typeNames.put(Character.valueOf(DataFieldMetadata.DECIMAL_FIELD), "CloverDecimal");
		typeNames.put(Character.valueOf(DataFieldMetadata.INTEGER_FIELD), "CloverInteger");
		typeNames.put(Character.valueOf(DataFieldMetadata.LONG_FIELD), "CloverLong");
		typeNames.put(Character.valueOf(DataFieldMetadata.NUMERIC_FIELD), "CloverNumeric");
		typeNames.put(Character.valueOf(DataFieldMetadata.STRING_FIELD), "CloverString");
		typeNames.put(Character.valueOf(DataFieldMetadata.BOOLEAN_FIELD), "CloverBoolean");

		primitiveNames.put(Character.valueOf(DataFieldMetadata.BYTE_FIELD), "xsd:base64Binary");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.BYTE_FIELD_COMPRESSED), "xsd:base64Binary");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.DATE_FIELD), "xsd:date");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.DATETIME_FIELD), "xsd:dateTime");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.DECIMAL_FIELD), "xsd:decimal");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.INTEGER_FIELD), "xsd:int");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.LONG_FIELD), "xsd:long");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.NUMERIC_FIELD), "xsd:decimal");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.STRING_FIELD), "xsd:string");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.BOOLEAN_FIELD), "xsd:boolean");
	}

	
}
