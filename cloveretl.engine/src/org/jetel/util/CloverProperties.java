/*
 * Created on May 1, 2003
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package org.jetel.util;

import org.jetel.metadata.DataFieldMetadata;

/**
 * @author administrator
 *
 * This is a placeholder for future full blown property class implementation.
 */
public final class CloverProperties {
	private static String[] getTypeNameList={"String","Date","Numeric","Integer"};
	
	public final static String getTypeName(char c) {
		switch (c) {
			case DataFieldMetadata.STRING_FIELD:
				return "String";
			case DataFieldMetadata.DATE_FIELD:
				return "Date";
			case DataFieldMetadata.DATETIME_FIELD:
				return "DateTime";
			case DataFieldMetadata.NUMERIC_FIELD:
				return "Numeric";
			case DataFieldMetadata.INTEGER_FIELD:
				return "Integer";
			case DataFieldMetadata.DECIMAL_FIELD:
				return "Decimal";
			case DataFieldMetadata.BYTE_FIELD:
				return "Byte";
		}
		return null;
		
	}


	public final static char getTypeOfName(String typeName) {
		if (typeName.equals("String"))
			return  DataFieldMetadata.STRING_FIELD;
		if (typeName.equals("Date"))
			return  DataFieldMetadata.DATE_FIELD;
		if (typeName.equals("DateTime"))
			return  DataFieldMetadata.DATETIME_FIELD;
		if (typeName.equals("Numeric"))
			return  DataFieldMetadata.NUMERIC_FIELD;
		if (typeName.equals("Integer"))
			return  DataFieldMetadata.INTEGER_FIELD;
		if (typeName.equals("Decimal"))
			return  DataFieldMetadata.DECIMAL_FIELD;
		if (typeName.equals("Byte"))
			return  DataFieldMetadata.BYTE_FIELD;
			
		return ' ';
	}


	/**
	 * @return
	 */
	public final static String[] getTypeNameList() {
		return getTypeNameList;
	}
}
