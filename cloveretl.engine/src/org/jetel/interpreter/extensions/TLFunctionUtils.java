package org.jetel.interpreter.extensions;

import java.util.Calendar;

import org.jetel.interpreter.TransformLangParserConstants;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;

public class TLFunctionUtils implements TransformLangParserConstants  {

	public static int astToken2CalendarField(TLValue astToken) {
		if (astToken.type!= TLValueType.SYM_CONST){
			throw new IllegalArgumentException("Argument is not of SYMBOLIC CONSTANT type: " + astToken);
		}
		int field = 0;
		switch (astToken.getInt()) {
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
		if (astToken.type!= TLValueType.SYM_CONST){
			throw new IllegalArgumentException("Argument is not of SYMBOLIC CONSTANT type: " + astToken);
		}
		
		switch(astToken.getInt()){
			case INT_VAR: return TLValueType.INTEGER;
			  case LONG_VAR: return TLValueType.LONG ;
			  case DATE_VAR: return TLValueType.DATE ;
			  case DOUBLE_VAR: return TLValueType.DOUBLE ;
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
	
}
