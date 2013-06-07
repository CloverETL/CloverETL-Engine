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
 *
 * @created May 25, 2010
 */
public class TLCalendarCache extends TLCache {
	
	Calendar cachedCalendar;
	private Object previousLocale;

	public TLCalendarCache() {
		cachedCalendar = MiscUtils.getDefaultCalendar();
	}
	
	public TLCalendarCache(TLFunctionCallContext context, int position) {
		createCachedCalendar(context, position);
	}
		
		 
	/**
	 * @param context
	 * @param position
	 */
	private void createCachedCalendar(TLFunctionCallContext context, int position) {
		
		if ((context.getLiteralsSize()) > position && context.isLiteral(position)) {
			String localeStr = (String) context.getParamValue(position);
			cachedCalendar = MiscUtils.createCalendar(localeStr, null); // TODO time zone as parameter?
		} else {
			cachedCalendar = MiscUtils.getDefaultCalendar();
		}
	}

	
	public Calendar getCalendar() {
		return cachedCalendar;
	}
	
	public Calendar getCachedCalendar(TLFunctionCallContext context, String locale, int position) {
		if (context.isLiteral(position) || (cachedCalendar != null && locale.equals(previousLocale))) {
			return cachedCalendar;
		} else {
			cachedCalendar = MiscUtils.createCalendar(locale, null);
			previousLocale = locale;
			return cachedCalendar;
		}
	}
}
