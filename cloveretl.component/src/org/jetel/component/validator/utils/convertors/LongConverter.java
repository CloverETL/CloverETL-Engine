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
package org.jetel.component.validator.utils.convertors;

import java.util.Date;

import org.jetel.component.validator.rules.NumberValidationRule;
import org.jetel.data.primitive.Decimal;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.CloverString;

/**
 * Implementation of converter for converting something to long.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 15.1.2013
 * @see NumberValidationRule
 */
public class LongConverter implements Converter {
	
	private static LongConverter instance;
	private LongConverter() {}
	
	/**
	 * Returns instance of converter
	 * @return Instance of long converter
	 */
	public static LongConverter getInstance() {
		if(instance == null) {
			instance = new LongConverter();
		}
		return instance;
	}

	@Override
	public Long convert(Object o) {
		if(o == null) {
			return null;
		}
		if(o instanceof Long) {
			return (Long) o;
		}
		if(o instanceof Integer) {
			return Long.valueOf(((Integer) o).longValue());
		}
		if(o instanceof Boolean) {
			return Long.valueOf(((Boolean) o).booleanValue() ? 1l : 0l);
		}
		if(o instanceof CloverString) {
			try {
				return Long.valueOf(((CloverString) o).toString());
			} catch (NumberFormatException e) {
				return null;
			}
		}
		if(o instanceof String) {
			try {
				return Long.valueOf((String) o);
			} catch (Exception e) {
				return null;
			}
		}
		if(o instanceof Decimal || o instanceof Number) {
			// It is sometime convertible, decision to not do it as behaviour would be unpredictible
			return null;
		}
		if(o instanceof byte[]) {
			// Decision to take length instead of content
			return Long.valueOf(((byte[]) o).length);
		}
		if(o instanceof Date) {
			return ((Date) o).getTime();
		}
		return null;
	}
	
	/**
	 * Inspired by {@link ComponentXMLAttributes.getLong(String)}
	 */
	@Override
	public Long convertFromCloverLiteral(String o) {
        if (o.equalsIgnoreCase(ComponentXMLAttributes.STR_MIN_LONG)){
            return Long.MIN_VALUE;
        } else if (o.equalsIgnoreCase(ComponentXMLAttributes.STR_MAX_LONG)){
            return Long.MAX_VALUE;
        }
        try {
        	return Long.parseLong(o);
        } catch(Exception ex){
        	return null; 
        }
	}

}
