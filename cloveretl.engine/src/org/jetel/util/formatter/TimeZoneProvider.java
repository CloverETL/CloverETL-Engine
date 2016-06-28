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

import org.jetel.util.CloverPublicAPI;
import org.jetel.util.MiscUtils;
import org.jetel.util.string.StringUtils;
import org.joda.time.DateTimeZone;
import org.threeten.bp.ZoneId;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created May 28, 2013
 */
@CloverPublicAPI
public class TimeZoneProvider implements Serializable {
	
	private static final long serialVersionUID = -1794232110904205542L;
	
	public static final String JODA_PREFIX = "joda:";
	public static final String JAVA_PREFIX = "java:";
	public static final String JAVA8_PREFIX = "java8:";
	private static final String ISO8601_PREFIX = "iso-8601:";

	private final TimeZone javaTimeZone;
	
	private final DateTimeZone jodaTimeZone;
	
	private final ZoneId java8TimeZone;
	
	private final String config;
	
	/**
	 * Creates a {@link TimeZoneProvider} using the default time zone.
	 */
	public TimeZoneProvider() {
		this((String) null);
	}
	
	/**
	 * If timeZoneStr is <code>null</code> or empty,
	 * creates a default time zone.
	 * Otherwise parses the provided string.
	 * 
	 * @param timeZoneStr
	 */
	public TimeZoneProvider(String timeZoneStr) {
		if (StringUtils.isEmpty(timeZoneStr)) {
			timeZoneStr = MiscUtils.getDefaultTimeZone();
		}
		this.config = timeZoneStr;
		
		DateTimeZone joda = null;
		TimeZone java = null;
		ZoneId java8 = null;
		String[] parts = StringUtils.split(timeZoneStr, ";");
		if (parts != null) {
			for (String id: parts) {
				id = StringUtils.unquote(id);
				
				if (StringUtils.isEmpty(id)) {
					throw new IllegalArgumentException("Invalid time zone specification: " + timeZoneStr);
				}
				
				if (id.startsWith(JODA_PREFIX)) {
					joda = DateTimeZone.forID(id.substring(JODA_PREFIX.length()));
				} else if (id.startsWith(ISO8601_PREFIX)) {
					joda = DateTimeZone.forID(id.substring(ISO8601_PREFIX.length()));
				} else if (id.startsWith(JAVA8_PREFIX)) {
					java8= ZoneId.of(id.substring(JAVA8_PREFIX.length()));
				} else {
					if (id.startsWith(JAVA_PREFIX)) {
						id = id.substring(JAVA_PREFIX.length());
					}
					java = TimeZone.getTimeZone(id);
				}
			}
		}
		this.javaTimeZone = java;
		this.jodaTimeZone = joda;
		this.java8TimeZone = java8;
	}
	
	/**
	 * @return the javaTimeZone
	 */
	public java.util.TimeZone getJavaTimeZone() {
		if (javaTimeZone == null) {
			throw new TimeZoneUndefinedException("No Java time zone has been set: " + config);
		}
		return javaTimeZone;
	}

	/**
	 * @return the jodaTimeZone
	 */
	public DateTimeZone getJodaTimeZone() {
		if (jodaTimeZone == null) {
			throw new TimeZoneUndefinedException("No \"joda:\" time zone has been set: " + config);
		}
		return jodaTimeZone;
	}

	/**
	 * @return the java8TimeZone
	 */
	public ZoneId getJava8TimeZone() {
		if (java8TimeZone == null) {
			throw new TimeZoneUndefinedException("No \"java8:\" time zone has been set: " + config);
		}
		return java8TimeZone;
	}

	@Override
	public String toString() {
		return config;
	}
	
	/**
	 * Returns true if timezone string contained at least one Java timezone.
	 * @return
	 */
	public boolean hasJavaTimeZone() {
		return this.javaTimeZone != null;
	}
	
	/**
	 * Returns true it timezone string contained at least on Joda timezone.
	 * @return
	 */
	public boolean hasJodaTimeZone() {
		return this.jodaTimeZone != null;
	}

	/**
	 * Returns true it timezone string contained at least on Java8 timezone.
	 * @return
	 */
	public boolean hasJava8TimeZone() {
		return this.java8TimeZone != null;
	}

	/**
	 * A subclass of {@link IllegalArgumentException},
	 * so that the specific exception can be caught during validation.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Jul 3, 2013
	 */
	public static class TimeZoneUndefinedException extends IllegalStateException {

		private static final long serialVersionUID = -177491851163652666L;

		public TimeZoneUndefinedException(String s) {
			super(s);
		}
		
	}

}
