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


/**
 * Enumeration of types of flat file parsing.
 * DELIMITED, FIXEDLEN, MIXED
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 16 Jan 2012
 */
public enum DataRecordParsingType {

	/**
	 * Data fields are delimited.
	 */
	@SuppressWarnings("deprecation")
	DELIMITED(DataRecordMetadata.DELIMITED_RECORD),
	
	/**
	 * Data fields are fixed length.
	 */
	@SuppressWarnings("deprecation")
	FIXEDLEN(DataRecordMetadata.FIXEDLEN_RECORD),
	
	/**
	 * Data fields are either delimited or fixed length.
	 */
	@SuppressWarnings("deprecation")
	MIXED(DataRecordMetadata.MIXED_RECORD);

	private char obsoleteIdentifier;
	
	private DataRecordParsingType(char obsoleteIdentifier) {
		this.obsoleteIdentifier = obsoleteIdentifier;
	}
	
	/**
	 * @return obsolete characted identification
	 * @deprecated
	 */
	@Deprecated
	public char getObsoleteIdnetifier() {
		return obsoleteIdentifier;
	}
	
	/**
	 * @param recType
	 * @return parsing type based on obsolete character identification
	 * @deprecated
	 */
	@Deprecated
	public static DataRecordParsingType fromChar(char charIdentifier) {
		for (DataRecordParsingType flatDataType : values()) {
			if (flatDataType.obsoleteIdentifier == charIdentifier) {
				return flatDataType;
			}
		}
		
		throw new IllegalArgumentException("Unknown flat data type '" + charIdentifier + "'.");
	}

}
