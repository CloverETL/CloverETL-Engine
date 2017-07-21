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
package org.jetel.data;

import java.sql.Timestamp;
import java.util.Date;

import org.jetel.util.CloverPublicAPI;
import org.jetel.util.EqualsUtil;
import org.jetel.util.HashCodeUtil;
import org.threeten.bp.Instant;

/**
 * Clover specific representation of an instant with nanoseconds precision.
 * This class is mutable and not thread safe.
 * 
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27. 6. 2016
 */
@CloverPublicAPI
public class NanoDate implements Comparable<NanoDate> {

	private static final int MILLIS_PER_SECOND = 1000;

    /**
     * The number of seconds from the epoch of 1970-01-01T00:00:00Z.
     */
	private long seconds;
	
    /**
     * The number of nanoseconds, later along the time-line, from the seconds field.
     * This is always positive, and never exceeds 999,999,999.
     */
	private int nanos;

	/**
	 * Cache for {@link #getInstant()} method. Needs to be cleared on each date-time update.
	 */
	private Instant instantCache;
	
	/**
	 * @return new {@link NanoDate} instance for the given {@link Date}
	 */
	public static NanoDate from(Date date) {
		return new NanoDate(date);
	}

	/**
	 * @return new {@link NanoDate} instance for the given {@link Timestamp}
	 */
	public static NanoDate from(Timestamp timestamp) {
		return new NanoDate(timestamp);
	}

	/**
	 * Constructs a {@link NanoDate} with 1970-01-01T00:00:00Z time.
	 */
	public NanoDate() {
	}

	/**
	 * Duplicates the given {@link NanoDate}.
	 */
	public NanoDate(NanoDate nanoDate) {
		this.seconds = nanoDate.seconds;
		this.nanos = nanoDate.nanos;
		this.instantCache = nanoDate.instantCache;
	}

	/**
	 * Constructs new {@link NanoDate}, date-time will be initialised by the given {@link Date}.
	 */
	public NanoDate(Date date) {
		setTime(date);
	}

	/**
	 * Constructs new {@link NanoDate}, date-time will be initialised by the given {@link Timestamp}.
	 */
	public NanoDate(Timestamp timestamp) {
		setTime(timestamp);
	}

	/**
	 * @return number of seconds from the epoch of 1970-01-01T00:00:00Z.
	 */
	public long getSeconds() {
		return seconds;
	}
	
	/**
	 * @return The number of nanoseconds, later along the time-line, from the seconds field.
	 * This is always positive, and never exceeds 999,999,999.
	 */
	public int getNanos() {
		return nanos;
	}
	
	/**
	 * @return {@link Instant} representing this {@link NanoDate}
	 * WARNING: the return value <code>org.threeten.bp.Instant</code>
	 * will be changed to <code>java.time.Instant</code> after full support of java 8 by clover
	 */
	public Instant getInstant() {
		if (instantCache == null) {
			instantCache = Instant.ofEpochSecond(seconds, nanos);
		}
		return instantCache;
	}
	
	/**
	 * Sets the time from the given {@link Instant}.
	 */
	void setTime(Instant instant) {
		instantCache = instant;
		this.seconds = instantCache.getEpochSecond();
		this.nanos = instantCache.getNano();
	}
	
	/**
	 * Sets the time from the given {@link NanoDate}.
	 */
	void setTime(NanoDate nanoDate) {
		this.seconds = nanoDate.seconds;
		this.nanos = nanoDate.nanos;
		instantCache = nanoDate.instantCache;
	}

	/**
	 * Sets the time from the given {@link Date}.
	 */
	void setTime(Date date) {
		setTime(Instant.ofEpochMilli(date.getTime()));
	}

	/**
	 * Sets the time from the given {@link Timestamp}.
	 */
	void setTime(Timestamp timestamp) {
		setTime(Instant.ofEpochSecond(timestamp.getTime() / MILLIS_PER_SECOND, timestamp.getNanos()));
	}
	
	/**
	 * Sets the time from the given epoch seconds and nanoseconds.
	 * @see #getSeconds()
	 * @see #getNanos()
	 */
	void setTime(long seconds, int nanos) {
		this.seconds = seconds;
		this.nanos = nanos;
		this.instantCache = null;
	}
	
	/**
	 * Resets the time to epoch start.
	 */
	void setEpoch() {
		seconds = 0;
		nanos = 0;
		instantCache = null;
	}
	
	@Override
	public boolean equals(Object aThat) {
		if (this == aThat) {
			return true;
		}
		if (!(aThat instanceof NanoDate)) {
			return false;
		}
		NanoDate that = (NanoDate) aThat;
		return EqualsUtil.areEqual(this.seconds, that.seconds)
				&& EqualsUtil.areEqual(this.nanos, that.nanos);
	}
	
	@Override
	public int hashCode() {
		int result = HashCodeUtil.SEED;
		result = HashCodeUtil.hash(result, seconds);
		result = HashCodeUtil.hash(result, nanos);
		return result;
	}

	@Override
	public int compareTo(NanoDate aThat) {
	    if (this == aThat) return 0;

	    if (this.seconds < aThat.seconds) return -1;
	    if (this.seconds > aThat.seconds) return 1;

	    if (this.nanos < aThat.nanos) return -1;
	    if (this.nanos > aThat.nanos) return 1;

	    return 0;
	}
	
}
