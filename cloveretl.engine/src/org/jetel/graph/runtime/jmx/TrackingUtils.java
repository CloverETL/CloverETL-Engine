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
	/**
	 * Converts time from the unit used to store it in tracking (nanoseconds) to the specified unit.
	 * @param time
	 * @param unit
	 * @return converted time.
	 */
	public static long converTime(long time, TimeUnit unit) {
		return unit.convert(time, TimeUnit.NANOSECONDS);
	}
}
