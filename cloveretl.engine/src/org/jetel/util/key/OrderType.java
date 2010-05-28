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

import org.jetel.exception.JetelException;

/**
 * Enumeration representing various type of data ordering.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.10.2009
 */
public enum OrderType {

	ASCENDING("a"),
	DESCENDING("d"),
	IGNORE("i"),
	AUTO("r");
	
	private String abbr;
	
	private OrderType(String abbr) {
		this.abbr = abbr;
	}
	
	/**
	 * @return abbreviation of order type; for instance "a" for ASCENDING order 
	 */
	public String getAbbr() {
		return abbr;
	}
	
	/**
	 * Convert the given string to this order type.
	 * @param value string form of order type
	 * @return order type
	 * @throws JetelException if the given string does not represent any known type of ordering
	 */
	public static OrderType valueFrom(String value) throws JetelException {
		for (OrderType orderType : values()) {
			if (isOrderType(value, orderType)) {
				return orderType;
			}
		}
		throw new JetelException("Unknown order type '" + value + "'.");
	}
	
	private static boolean isOrderType(String value, OrderType orderType) {
		return value.substring(0, orderType.getAbbr().length()).equalsIgnoreCase(orderType.getAbbr());
	}
	
}
