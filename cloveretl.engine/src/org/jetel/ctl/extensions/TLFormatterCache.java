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

import java.util.Locale;
import java.util.Objects;

import org.jetel.util.MiscUtils;
import org.jetel.util.formatter.TimeZoneProvider;
import org.jetel.util.string.StringUtils;

/**
 * A base {@link TLCache} implementation that
 * can store {@link TLFunctionCallContext}
 * and caches {@link Locale} and {@link TimeZoneProvider}.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29. 8. 2014
 */
public abstract class TLFormatterCache extends TLCache {

	protected String previousLocaleString;
	protected String previousTimeZoneString;
	
	private Locale previousLocale;
	private TimeZoneProvider previousTimeZone;

	private final TLFunctionCallContext context;

	protected TLFormatterCache(TLFunctionCallContext context) {
		this.context = context;
		this.previousLocale = context.getDefaultLocale();
		this.previousTimeZone = context.getDefaultTimeZone();
	}

	/**
	 * Returns a {@link Locale} instance
	 * for the given locale ID.
	 * If the input string is <code>null</code> or empty,
	 * returns default Locale from the context.
	 * 
	 * <p>Examples:<br>
	 * en en.GB fr fr.FR cs cs.CZ
	 * </p>
	 * 
	 * Caches the last instance.
	 * A call to this method updates
	 * {@link #previousLocaleString} as a side effect.
	 * 
	 * @param localeString	locale ID
	 * @return cached {@link Locale}
	 */
	protected Locale getLocale(String localeString) {
		Locale locale = previousLocale;
		if (!Objects.equals(localeString, previousLocaleString)) {
			if (StringUtils.isEmpty(localeString)) {
				locale = context.getDefaultLocale();
			} else {
				locale = MiscUtils.createLocale(localeString);
			}
		}
		previousLocaleString = localeString;
		previousLocale = locale;
		return locale;
	}
	
	/**
	 * Returns a {@link TimeZoneProvider} instance
	 * for the given time zone string. 
	 * 
	 * If the input string is <code>null</code> or empty,
	 * returns default TimeZoneProvider from the context.

	 * <p>Examples:<br>
	 * Europe/Prague America/Los_Angeles
	 * </p>
	 * 
	 * Caches the last instance.
	 * A call to this method updates
	 * {@link #previousTimeZoneString} as a side effect.
	 * 
	 * @param timeZoneString	time zone string
	 * @return cached {@link TimeZoneProvider}
	 */
	protected TimeZoneProvider getTimeZone(String timeZoneString) {
		TimeZoneProvider timeZone = previousTimeZone;
		if (!Objects.equals(timeZoneString, previousTimeZoneString)) {
			if (StringUtils.isEmpty(timeZoneString)) {
				timeZone = context.getDefaultTimeZone();
			} else {
				timeZone = new TimeZoneProvider(timeZoneString);
			}
		}
		previousTimeZoneString = timeZoneString;
		previousTimeZone = timeZone;
		return timeZone;
	}

	
}
