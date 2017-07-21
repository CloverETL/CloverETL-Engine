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
package org.jetel.graph.runtime.jmx;

import java.util.concurrent.TimeUnit;

/**
 * Utility methods for tracking.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlin.eu)
 *         (c) Javlin Consulting (www.javlin.cz)
 *
 * @since May 19, 2009
 */
public class TrackingUtils {
	
	public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
	
	/**
	 * Converts time from the unit used to store it in tracking (milliseconds) to the specified unit.
	 * @param time
	 * @param unit
	 * @return converted time.
	 */
	public static long convertTime(long time, TimeUnit targetUnit) {
		return convertTime(time, DEFAULT_TIME_UNIT, targetUnit);
	}
	
	/**
	 * Converts time from the sourceUnit to the targetUnit.
	 * @param time
	 * @param sourceUnit
	 * @param targetUnit
	 * @return converted time.
	 */
	public static long convertTime(long time, TimeUnit sourceUnit, TimeUnit targetUnit) {
		return targetUnit.convert(time, sourceUnit);
	}

}
