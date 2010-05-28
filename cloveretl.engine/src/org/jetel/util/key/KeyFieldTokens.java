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
package org.jetel.util.key;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.exception.JetelException;
import org.jetel.util.string.StringUtils;

/**
 * String based representation of a key field with ordering type.
 * Result of tokenization at {@link KeyTokenizer}.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.10.2009
 */
public class KeyFieldTokens {

	private String fieldName;

	private OrderType orderType;
	
	/**
	 * 
	 */
	public KeyFieldTokens(String fieldName, OrderType orderType) {
		this.fieldName = fieldName;
		this.orderType = orderType;
	}

	public String getFieldName() {
		return fieldName;
	}

	/**
	 * @return order type or null if was not specified
	 */
	public OrderType getOrderType() {
		return orderType;
	}

	static KeyFieldTokens parseKeyField(String keyFieldString) throws JetelException {
		Pattern keyPartPattern = Pattern.compile("^([^\\(\\)]*?)(\\(([^\\(\\)]*)\\))?$");
		  
		Matcher matcher = keyPartPattern.matcher(keyFieldString);
		if (matcher.matches()) {
			String fieldName = matcher.group(1);
			OrderType orderType = null;
			if (!StringUtils.isEmpty(matcher.group(3))) {
				orderType = OrderType.valueFrom(matcher.group(3));	        		
			}
			return new KeyFieldTokens(fieldName, orderType);
		} else {
			throw new JetelException("Invalid key field format '" + keyFieldString + "'.");
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		if (orderType != null) {
			return fieldName + "(" + orderType.getAbbr() + ")";
		} else {
			return fieldName;
		}
	}
	
}
