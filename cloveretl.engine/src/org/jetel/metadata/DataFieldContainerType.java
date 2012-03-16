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
 * Container type of data field - SINGLE, LIST and MAP.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17 Jan 2012
 */
public enum DataFieldContainerType {

	SINGLE("single"), LIST("list"), MAP("map");
	
	private final String displayName;
	
	private DataFieldContainerType(String displayName) {
		this.displayName = displayName;
	}
	
	/**
	 * @param strType
	 * @return container type based on string without case sensitivity
	 */
	public static DataFieldContainerType fromString(String strType) {
		for (DataFieldContainerType type : values()) {
			if (type.name().equalsIgnoreCase(strType)) {
				return type;
			}
		}
		
		throw new IllegalArgumentException("Uknown type of data field container '" + strType + "'.");
	}

	/**
	 * @return the display name of the container type
	 */
	public String getDisplayName() {
		return displayName;
	}
	
	/**
	 * Returns an array containing all display names.
	 * @return
	 */
	public static String[] getDisplayNames() {
		DataFieldContainerType[] values = values();
		String[] result = new String[values.length];
		for (int i = 0; i < values.length; i++) {
			result[i] = values[i].displayName;
		}
		return result;
	}
	
}
