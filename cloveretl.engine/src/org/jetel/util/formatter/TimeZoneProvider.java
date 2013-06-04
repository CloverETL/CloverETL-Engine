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
package org.jetel.util.formatter;

import java.io.Serializable;
import java.util.TimeZone;

import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.util.string.StringUtils;
import org.joda.time.DateTimeZone;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created May 28, 2013
 */
public class TimeZoneProvider implements Serializable {
	
	private static final long serialVersionUID = -1794232110904205542L;
	
	private static final String JODA_PREFIX = "joda:";

	private final TimeZone javaTimeZone;
	
	private final DateTimeZone jodaTimeZone;
	
	private final String config;
	
	/**
	 * Creates a {@link TimeZoneProvider} using the default time zone.
	 */
	public TimeZoneProvider() {
		this((String) null);
	}
	
	/**
	 * @param timeZone Java time zone
	 */
	public TimeZoneProvider(TimeZone timeZone) {
		if (timeZone == null) {
			throw new NullPointerException("timeZone is null");
		}
		this.javaTimeZone = timeZone;
		this.jodaTimeZone = null;
		this.config = timeZone.getID();
	}

	/**
	 * @param timeZone Joda time zone
	 */
	public TimeZoneProvider(DateTimeZone timeZone) {
		if (timeZone == null) {
			throw new NullPointerException("timeZone is null");
		}
		this.javaTimeZone = null;
		this.jodaTimeZone = timeZone;
		this.config = JODA_PREFIX + timeZone.getID();
	}
	
	/**
	 * If timeZoneStr is <code>null</code> or empty,
	 * creates a default time zone.
	 * Otherwise parses the provided string.
	 * 
	 * @param timeZoneStr
	 */
	public TimeZoneProvider(String timeZoneStr) {
		if (timeZoneStr == null) {
			timeZoneStr = GraphRuntimeContext.getDefaultTimeZone();
		}
		this.config = timeZoneStr;
		
		DateTimeZone joda = null;
		TimeZone java = null;
		String[] parts = StringUtils.split(timeZoneStr, ";");
		if (parts != null) {
			for (String id: parts) {
				id = StringUtils.unquote(id);
				
				if (StringUtils.isEmpty(id)) {
					throw new IllegalArgumentException("Invalid time zone specification: " + timeZoneStr);
				}
				
				if (id.startsWith(JODA_PREFIX)) {
					joda = DateTimeZone.forID(id.substring(JODA_PREFIX.length()));
				} else {
					java = TimeZone.getTimeZone(id);
				}
			}
		}
		this.javaTimeZone = java;
		this.jodaTimeZone = joda;
	}
	
	/**
	 * @return the javaTimeZone
	 */
	public java.util.TimeZone getJavaTimeZone() {
		if (javaTimeZone == null) {
			throw new IllegalStateException("No Java time zone has been set: " + config);
		}
		return javaTimeZone;
	}

	/**
	 * @return the jodaTimeZone
	 */
	public DateTimeZone getJodaTimeZone() {
		if (jodaTimeZone == null) {
			throw new IllegalStateException("No \"joda:\" time zone has been set: " + config);
		}
		return jodaTimeZone;
	}

	@Override
	public String toString() {
		return config;
	}

}
