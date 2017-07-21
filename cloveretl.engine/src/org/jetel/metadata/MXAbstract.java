/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.metadata;

import java.util.HashMap;

abstract public class MXAbstract {

	// namespaces
	protected static final String[] NAMESPACES = new String[] {"xsd", "xs"};
	protected static final String NAMESPACE_DELIMITER = ":";

	// xsd
	protected static final String NAMESPACE = "";
	protected static final String XMLSCHEMA = "http://www.w3.org/2001/XMLSchema";
	protected static final String XMLNS_XSD = "xmlns:xsd";
	protected static final String XMLNS = "xmlns";

	// xsd elements
	protected static final String XSD_SCHEMA = "schema";
	protected static final String XSD_ELEMENT = "element";
	protected static final String XSD_COMPLEX_TYPE = "complexType";
	protected static final String XSD_SEQUENCE = "sequence";
	protected static final String XSD_SIMPLE_TYPE = "simpleType";
	protected static final String XSD_RESTRICTION = "restriction";
	
	// xsd restrictions
	protected static final String XSD_LENGHT = "length";
	protected static final String XSD_PATTERN = "pattern";
	protected static final String XSD_FRACTION_DIGITS = "fractionDigits";
	protected static final String XSD_TOTAL_DIGITS = "totalDigits";
	
	// xsd base data types
	protected static final String XSD_DECIMAL = "decimal";
	protected static final String XSD_BASE64BINARY = "base64Binary";

	// xsd attributes
	protected static final String NAME = "name";
	protected static final String TYPE = "type";
	protected static final String BASE = "base";
	protected static final String VALUE = "value";
	protected static final String MIN_OCCURS = "minOccurs";
	protected static final String MAX_OCCURS = "maxOccurs";

	// record attributes
	protected static final String LENGTH = "length";
	protected static final String SCALE = "scale";
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
	
	
	// mapping from field type to XSD type representing the field type
	protected static final HashMap<Character, String> typeNames = new HashMap<Character, String>();
	
	// mapping from field type to XSD type used as base for XSD type representing the field type
	protected static final HashMap<Character, String> primitiveNames = new HashMap<Character, String>();
	
	// inverse primitiveNames mapping
	protected static final HashMap<String, Character> namesPrimitive = new HashMap<String, Character>();

	// initialize mappings
	protected static final String CLOVER_BYTE_COMPRESSED = "CloverByteCompressed";
	static {
		typeNames.put(Character.valueOf(DataFieldMetadata.BYTE_FIELD), "CloverByte");
		typeNames.put(Character.valueOf(DataFieldMetadata.BYTE_FIELD_COMPRESSED), CLOVER_BYTE_COMPRESSED);
		typeNames.put(Character.valueOf(DataFieldMetadata.DATE_FIELD), "CloverDate");
		typeNames.put(Character.valueOf(DataFieldMetadata.DATETIME_FIELD), "CloverDatetime");
		typeNames.put(Character.valueOf(DataFieldMetadata.DECIMAL_FIELD), "CloverDecimal");
		typeNames.put(Character.valueOf(DataFieldMetadata.INTEGER_FIELD), "CloverInteger");
		typeNames.put(Character.valueOf(DataFieldMetadata.LONG_FIELD), "CloverLong");
		typeNames.put(Character.valueOf(DataFieldMetadata.NUMERIC_FIELD), "CloverNumeric");
		typeNames.put(Character.valueOf(DataFieldMetadata.STRING_FIELD), "CloverString");
		typeNames.put(Character.valueOf(DataFieldMetadata.BOOLEAN_FIELD), "CloverBoolean");

		primitiveNames.put(Character.valueOf(DataFieldMetadata.BYTE_FIELD), XSD_BASE64BINARY);
		primitiveNames.put(Character.valueOf(DataFieldMetadata.BYTE_FIELD_COMPRESSED), XSD_BASE64BINARY);
		primitiveNames.put(Character.valueOf(DataFieldMetadata.DATE_FIELD), "date");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.DATETIME_FIELD), "dateTime");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.DECIMAL_FIELD), XSD_DECIMAL);
		primitiveNames.put(Character.valueOf(DataFieldMetadata.INTEGER_FIELD), "int");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.LONG_FIELD), "long");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.NUMERIC_FIELD), XSD_DECIMAL);
		primitiveNames.put(Character.valueOf(DataFieldMetadata.STRING_FIELD), "string");
		primitiveNames.put(Character.valueOf(DataFieldMetadata.BOOLEAN_FIELD), "boolean");
		
		for (Character ch: primitiveNames.keySet()) {
			if (ch == DataFieldMetadata.BYTE_FIELD_COMPRESSED || ch == DataFieldMetadata.NUMERIC_FIELD) {
				primitiveNames.put(ch, NAMESPACES[0] + NAMESPACE_DELIMITER + primitiveNames.get(ch));
				continue;
			}
			namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + primitiveNames.get(ch), ch);
			namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + primitiveNames.get(ch), ch);
			primitiveNames.put(ch, NAMESPACES[0] + NAMESPACE_DELIMITER + primitiveNames.get(ch));
		}
		
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "precisionDecimal", DataFieldMetadata.DECIMAL_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "precisionDecimal", DataFieldMetadata.DECIMAL_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "float", DataFieldMetadata.NUMERIC_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "float", DataFieldMetadata.NUMERIC_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "double", DataFieldMetadata.NUMERIC_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "double", DataFieldMetadata.NUMERIC_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "duration", DataFieldMetadata.LONG_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "duration", DataFieldMetadata.LONG_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "time", DataFieldMetadata.DATE_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "time", DataFieldMetadata.DATE_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "hexBinary", DataFieldMetadata.BYTE_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "hexBinary", DataFieldMetadata.BYTE_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "anyURI", DataFieldMetadata.STRING_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "anyURI", DataFieldMetadata.STRING_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "gYearMonth", DataFieldMetadata.INTEGER_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "gYearMonth", DataFieldMetadata.INTEGER_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "gYear", DataFieldMetadata.INTEGER_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "gYear", DataFieldMetadata.INTEGER_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "gMonthDay", DataFieldMetadata.INTEGER_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "gMonthDay", DataFieldMetadata.INTEGER_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "gDay", DataFieldMetadata.INTEGER_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "gDay", DataFieldMetadata.INTEGER_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "gMonth", DataFieldMetadata.INTEGER_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "gMonth", DataFieldMetadata.INTEGER_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "QName", DataFieldMetadata.STRING_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "QName", DataFieldMetadata.STRING_FIELD);
		namesPrimitive.put(NAMESPACES[0] + NAMESPACE_DELIMITER + "NOTATION", DataFieldMetadata.STRING_FIELD);
		namesPrimitive.put(NAMESPACES[1] + NAMESPACE_DELIMITER + "NOTATION", DataFieldMetadata.STRING_FIELD);
	}
	
}
