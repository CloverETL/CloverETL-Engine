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
package org.jetel.interpreter.extensions;

import java.util.Calendar;

import org.jetel.interpreter.TransformLangParserConstants;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;

public class TLFunctionUtils implements TransformLangParserConstants  {

	public static int astToken2CalendarField(TLValue astToken) {
		if (astToken.getType()!= TLValueType.SYM_CONST){
			throw new IllegalArgumentException("Argument is not of SYMBOLIC CONSTANT type: " + astToken);
		}
		int field = 0;
		switch (((TLNumericValue)astToken).getInt()) {
		case YEAR:
			field = Calendar.YEAR;
			break;
		case MONTH:
			field = Calendar.MONTH;
			break;
		case WEEK:
			field = Calendar.WEEK_OF_YEAR;
			break;
		case DAY:
			field = Calendar.DAY_OF_MONTH;
			break;
		case HOUR:
			field = Calendar.HOUR_OF_DAY;
			break;
		case MINUTE:
			field = Calendar.MINUTE;
			break;
		case SECOND:
			field = Calendar.SECOND;
			break;
		case MILLISEC:
			field = Calendar.MILLISECOND;
			break;
		default:
			throw new IllegalArgumentException("Token is not of CALENDAR type: " + astToken);

		}
		return field;
	}
	
	public static TLValueType astToken2ValueType(TLValue astToken){
		if (astToken.getType()!= TLValueType.SYM_CONST){
			throw new IllegalArgumentException("Argument is not of SYMBOLIC CONSTANT type: " + astToken);
		}
		
		switch(((TLNumericValue)astToken).getInt()){
			case INT_VAR: return TLValueType.INTEGER;
			  case LONG_VAR: return TLValueType.LONG ;
			  case DATE_VAR: return TLValueType.DATE ;
			  case DOUBLE_VAR: return TLValueType.NUMBER ;
			  case DECIMAL_VAR:  return TLValueType.DECIMAL;
			  case BOOLEAN_VAR: return TLValueType.BOOLEAN ;
			  case STRING_VAR: return TLValueType.STRING ;
			  case BYTE_VAR:  return TLValueType.BYTE;
			  case LIST_VAR:  return TLValueType.LIST;
			  case MAP_VAR:  return TLValueType.MAP;
			  case RECORD_VAR:  return TLValueType.RECORD;
			  case OBJECT_VAR:  return TLValueType.OBJECT;
			 default:
				 throw new IllegalArgumentException("Token is not of VARIABLE type: " + astToken);
		}
		
	}
	
	public static int valueType2astToken(TLValueType type){
		
		switch (type) {
		case INTEGER:	return TransformLangParserConstants.INT_VAR;
		case LONG:		return TransformLangParserConstants.LONG_VAR;
		case DATE:		return TransformLangParserConstants.DATE_VAR;
		case NUMBER:	return TransformLangParserConstants.DOUBLE_VAR;
		case DECIMAL:	return TransformLangParserConstants.DECIMAL_VAR;
		case BOOLEAN:	return TransformLangParserConstants.BOOLEAN_VAR;
		case STRING:	return TransformLangParserConstants.STRING_VAR;
		case BYTE:		return TransformLangParserConstants.BYTE_VAR;
		case LIST:		return TransformLangParserConstants.LIST_VAR;
		case MAP:		return TransformLangParserConstants.MAP_VAR;
		case RECORD:	return TransformLangParserConstants.RECORD_VAR;
		case OBJECT:	return TransformLangParserConstants.OBJECT_VAR;
		case SYM_CONST:return TransformLangParserConstants.ENUM;
		default:
			 throw new IllegalArgumentException("Unknown TL type: " + type.getName());
		}
	}
}
