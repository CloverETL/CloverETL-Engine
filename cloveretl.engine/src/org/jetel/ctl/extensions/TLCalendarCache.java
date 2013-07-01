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
package org.jetel.ctl.extensions;

import java.util.Calendar;

import org.jetel.util.MiscUtils;

/**
 * @author jakub (jakub.lehotsky@javlin.eu)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 * @author krivanekm
 *
 * @created May 25, 2010
 */
public class TLCalendarCache extends TLCache {
	
	private Calendar cachedCalendar;
	private String previousLocale;
	private String previousTimeZone;

	public TLCalendarCache() {
		cachedCalendar = MiscUtils.getDefaultCalendar();
	}
	
	public TLCalendarCache(TLFunctionCallContext context, int localePosition) {
		this(context, localePosition, -1);
	}
	
	public TLCalendarCache(TLFunctionCallContext context, int localePosition, int timeZonePosition) {
		createCachedCalendar(context, localePosition, timeZonePosition);
	}
	
	/**
	 * Equivalent to calling {@link #TLCalendarCache(TLFunctionCallContext, int, int)}
	 * with -1 as the second parameter.
	 * 
	 * @param context
	 * @param timeZonePosition
	 * @return new time-zone sensitive {@link TLCalendarCache}
	 */
	public static TLCalendarCache withTimeZone(TLFunctionCallContext context, int timeZonePosition) {
		return new TLCalendarCache(context, -1, timeZonePosition);
	}
	
	/**
	 * @param context
	 * @param localePosition
	 * @param timeZonePosition
	 */
	private void createCachedCalendar(TLFunctionCallContext context, int localePosition, int timeZonePosition) {
		String localeStr = null;
		String timeZoneStr = null;
		
		if ((localePosition >= 0) && (context.getLiteralsSize() > localePosition) && context.isLiteral(localePosition)) {
			localeStr = (String) context.getParamValue(localePosition);
		}
		
		if ((timeZonePosition >= 0) && (context.getLiteralsSize() > timeZonePosition) && context.isLiteral(timeZonePosition)) {
			Object timeZone = context.getParamValue(timeZonePosition);
			if (timeZone instanceof String) { // allow overriding; in compiled mode, context.getParams() returns null 
				timeZoneStr = (String) timeZone;
			}
		}
		
		cachedCalendar = MiscUtils.createCalendar(localeStr, timeZoneStr);
	}
	
	public Calendar getCalendar() {
		return cachedCalendar;
	}
	
	/**
	 * Returns cached locale-sensitive calendar instance.
	 * 
	 * @param context
	 * @param locale
	 * @param localePosition
	 * @return
	 */
	public Calendar getCachedCalendar(TLFunctionCallContext context, String locale, int localePosition) {
		if (context.isLiteral(localePosition) || (cachedCalendar != null && locale.equals(previousLocale))) {
			return cachedCalendar;
		} else {
			cachedCalendar = MiscUtils.createCalendar(locale, null);
			previousLocale = locale;
			return cachedCalendar;
		}
	}

	/**
	 * Returns cached time zone-sensitive calendar instance.
	 * 
	 * @param context
	 * @param timeZone
	 * @param timeZonePosition
	 * @return
	 */
	public Calendar getCachedCalendarWithTimeZone(TLFunctionCallContext context, String timeZone, int timeZonePosition) {
		if ((context.getLiteralsSize() > timeZonePosition) && context.isLiteral(timeZonePosition) || (cachedCalendar != null && ((timeZone == previousTimeZone) || timeZone.equals(previousTimeZone)))) {
			return cachedCalendar;
		} else {
			cachedCalendar = MiscUtils.createCalendar(null, timeZone);
			previousTimeZone = timeZone;
			return cachedCalendar;
		}
	}
}
