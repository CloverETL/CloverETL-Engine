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

package org.jetel.data.tree.bean;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25.10.2011
 */
public class SimpleTypes {

	private static final Set<Class<?>> SIMPLE_TYPES;
	static {
		SIMPLE_TYPES = new HashSet<Class<?>>();

		// Add primitive types
		SIMPLE_TYPES.add(boolean.class);
		SIMPLE_TYPES.add(byte.class);
		SIMPLE_TYPES.add(char.class);
		SIMPLE_TYPES.add(double.class);
		SIMPLE_TYPES.add(float.class);
		SIMPLE_TYPES.add(int.class);
		SIMPLE_TYPES.add(long.class);
		SIMPLE_TYPES.add(short.class);

		SIMPLE_TYPES.add(byte[].class);

		// Standard types
		SIMPLE_TYPES.add(BigDecimal.class);
		SIMPLE_TYPES.add(BigInteger.class);
		SIMPLE_TYPES.add(Boolean.class);
		SIMPLE_TYPES.add(Byte.class);
		SIMPLE_TYPES.add(Character.class);
		SIMPLE_TYPES.add(Double.class);
		SIMPLE_TYPES.add(Float.class);
		SIMPLE_TYPES.add(Integer.class);
		SIMPLE_TYPES.add(Long.class);
		SIMPLE_TYPES.add(Short.class);
		SIMPLE_TYPES.add(String.class);

		// Other types
		SIMPLE_TYPES.add(java.util.Date.class);
		SIMPLE_TYPES.add(Calendar.class);
		SIMPLE_TYPES.add(Timestamp.class);
		SIMPLE_TYPES.add(URL.class);

		/*
		 * FIXME
		 */
		SIMPLE_TYPES.add(Object.class);
	}
	
	private static final Map<String, Class<?>> PRIMITIVE_TYPES = new HashMap<String, Class<?>>();
	static {
		PRIMITIVE_TYPES.put(boolean.class.getName(), boolean.class);
		PRIMITIVE_TYPES.put(byte.class.getName(), byte.class);
		PRIMITIVE_TYPES.put(char.class.getName(), char.class);
		PRIMITIVE_TYPES.put(double.class.getName(), double.class);
		PRIMITIVE_TYPES.put(float.class.getName(), float.class);
		PRIMITIVE_TYPES.put(int.class.getName(), int.class);
		PRIMITIVE_TYPES.put(long.class.getName(), long.class);
		PRIMITIVE_TYPES.put(short.class.getName(), short.class);
	}
	
	private static final Set<String> SIMPLE_TYPES_NAMES = new HashSet<String>(SIMPLE_TYPES.size());
	static {
		for (Class<?> type : SIMPLE_TYPES) {
			SIMPLE_TYPES_NAMES.add(type.getName());
		}
	}
	
	public static boolean isSimpleType(Class<?> type) {
		return SIMPLE_TYPES.contains(type);
	}
	
	public static boolean isSimpleType(String typeName) {
		return SIMPLE_TYPES_NAMES.contains(typeName);
	}
	
	public static Class<?> getPrimitiveClass(String typeName) {
		return PRIMITIVE_TYPES.get(typeName);
	}
	
	public static boolean isPrimitiveType(String typeName) {
		return PRIMITIVE_TYPES.containsKey(typeName);
	}
}
