/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.util;

import org.jetel.metadata.DataFieldMetadata;

/**
 * @author administrator
 *
 * This is a placeholder for future full blown property class implementation.
 */
public final class CloverProperties {
	public final static String CLASS_DIRECTORY = "BIN";
	public final static String USER_JAVA_PACKAGE_NAME = "org.jetel.userclasses";
	
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
