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
package org.jetel.ctl.data;


/**
 * Auxiliary Enum to help "switch" based on function parameter type(s)
 * in CTL functions implementations (interpreted mode). CTL's TLType
 * does not have fixed enum-like list of all supported/available data types.
 * This Enum supports only the basic set. Should CTL/Clover data type be extended, this
 * enum should be enhanced accordingly.
 * 
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created May 16, 2013
 */
public enum TLTypeEnum {
	
	STRING, INT, LONG, DOUBLE, DECIMAL, DATE, BYTEARRAY, BOOLEAN, MAP, LIST, RECORD, UNKNOWN;
	
	public static TLTypeEnum convertParamType(TLType type) {
		if (type.isString()) {
			return STRING;
		} else if (type.isInteger()) {
			return INT;
		} else if (type.isLong()) {
			return LONG;
		} else if (type.isDecimal()) {
			return DECIMAL;
		} else if (type.isDouble()) {
			return DOUBLE;
		} else if (type.isByteArray()) {
			return BYTEARRAY;
		} else if (type.isDate()) {
			return DATE;
		} else if (type.isBoolean()) {
			return BOOLEAN;
		} else if (type.isMap()) {
			return MAP;
		} else if (type.isList()) {
			return LIST;
		} else if (type.isRecord()) {
			return RECORD;
		} else { // everything else- if there is any
			return UNKNOWN;
		}
	}

}
