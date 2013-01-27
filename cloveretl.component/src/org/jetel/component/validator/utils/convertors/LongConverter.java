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

import org.jetel.data.primitive.Decimal;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 15.1.2013
 */
public class LongConverter implements Converter {
	
	private static LongConverter instance;
	private LongConverter() {}
	
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
		if(o instanceof String) {
			try {
				return Long.valueOf((String) o);
			} catch (NumberFormatException e) {
				return null;
			}
		}
		if(o instanceof Decimal || o instanceof Number) {
			return null;
		}
		if(o instanceof byte[]) {
			return Long.valueOf(((byte[]) o).length);
		}
		if(o instanceof Date) {
			return ((Date) o).getTime();
		}
		return null;
	}

}
