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
package org.jetel.enums;

import org.jetel.util.string.StringUtils;

/**
 * This enum represents types of edge debugging.
 * This type is related to a particular edge.
 * 
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 23. 3. 2017
 */
public enum EdgeDebugMode {
	/** All records are cached. */
	ALL,
	/** Only first 1000 records are cached. */
	DEFAULT,
	/** No records are cached. */
	OFF,
	/** Only records specified with other conditions are cached. */
	CUSTOM;

	/**
	 * @return string representation of this edge debug mode
	 */
	public String getName() {
		return name().toLowerCase();
	}
	
	/**
	 * @param edgeDebugModeStr
	 * @return {@link EdgeDebugMode} instance based on the given string representation,
	 * DEFAULT is returned for unknown values
	 */
	public static EdgeDebugMode fromString(String edgeDebugModeStr) {
		if (!StringUtils.isEmpty(edgeDebugModeStr)) {
			for (EdgeDebugMode edgeDebugMode : values()) {
				if (edgeDebugMode.name().equalsIgnoreCase(edgeDebugModeStr)) {
					return edgeDebugMode;
				}
			}
	    	//backward compatibility - debug mode used to be just true/false value
			if ("true".equals(edgeDebugModeStr)) {
				return EdgeDebugMode.CUSTOM;
			}
		}
		return DEFAULT;
	}
	
}
