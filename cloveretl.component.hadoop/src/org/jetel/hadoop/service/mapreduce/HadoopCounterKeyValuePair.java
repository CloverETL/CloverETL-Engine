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
package org.jetel.hadoop.service.mapreduce;

/**
 * <p> Represents Hadoop map/reduce counter key/value pair where key is identification of Hadoop counter. A map/reduce
 * counter key-value pair consist of instance fo {@link HadoopCounterKey} and corresponding value for the key. </p>
 * 
 * <p> This class is immutable. Equality relation is defined on instances of this class by overriding methods
 * {@link Object#hashCode()} and {@link Object#equals(Object)}. Two instances are equal if and only if they have equal
 * counter key a and value. </p>
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 * @see HadoopJobReporter#getAllCounters()
 * @see HadoopJobReporter#getCustomCounters()
 * @see HadoopCounterKey
 */
public class HadoopCounterKeyValuePair {
	private HadoopCounterKey key;
	private long value;

	/**
	 * Constructs new counter key/value pair for given key and value.
	 * @param key An unambiguous identification of the counter.
	 * @param value Value of the counter.
	 */
	public HadoopCounterKeyValuePair(HadoopCounterKey key, long value) {
		if (key == null) {
			throw new NullPointerException("key");
		}
		this.value = value;
		this.key = key;
	}

	/**
	 * Creates new instance for specified counter name, group and value of the counter.
	 * @param counterName A name of the counter. Same as declared name of Hadoop API enum constant identifying the
	 *        counter.
	 * @param counterGroupName A group name of the counter. Same as fully qualified name of enum that declares Hadoop
	 *        API constant that identifies the counter.
	 * @param value A value of the counter.
	 */
	public HadoopCounterKeyValuePair(String counterName, String counterGroupName, long value) {
		this(counterName, counterGroupName, null, value);
	}

	/**
	 * Creates new instance for specified counter name, counter display name, group and value of the counter.
	 * @param counterName A name of the counter. Same as declared name of Hadoop API enum constant identifying the
	 *        counter.
	 * @param counterGroupName A group name of the counter. Same as fully qualified name of enum that declares Hadoop
	 *        API constant that identifies the counter.
	 * @param counterDiplayName Name of the counter intended to be displayed to users.
	 * @param value A value of the counter.
	 */
	public HadoopCounterKeyValuePair(String counterName, String counterGroupName, String counterDiplayName, long value) {
		this(new HadoopCounterKey(counterName, counterGroupName, counterDiplayName), value);
	}

	/**
	 * Gets key of the counter.
	 * @return Key of this counter key/value pair. Granted to be non-null.
	 */
	public HadoopCounterKey getKey() {
		return key;
	}

	/**
	 * Convenience method for getting name of key of this counter key/value pair. Equals to call
	 * <code>getKey().getName()</code>.
	 * @return Name of key of this counter key/value pair.
	 */
	public String getKeyName() {
		return key.getName();
	}

	/**
	 * Convenience method for getting display name of key of this counter key/value pair. Equals to call
	 * <code>getKey().getDisplayName()</code>.
	 * @return Display name of key of this counter key/value pair.
	 */
	public String getKeyDisplayName() {
		return key.getDisplayName();
	}

	/**
	 * Convenience method for getting group name of key of this counter key/value pair. Equals to call
	 * <code>getKey().getGroupName()</code>.
	 * @return Group name of key of this counter key/value pair.
	 */
	public String getCounterGroupName() {
		return getKey().getGroupName();
	}

	/**
	 * Gets value of the counter.
	 * @return A value of the counter as specified in constructor.
	 */
	public long getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + key.hashCode();
		result = prime * result + (int) (value ^ (value >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof HadoopCounterKeyValuePair)) {
			return false;
		}
		HadoopCounterKeyValuePair other = (HadoopCounterKeyValuePair) obj;
		return key.equals(other.key) && value == other.value;
	}

	/**
	 * Gets textual representation of this instance using key group name and key name for representing key of the
	 * counter.
	 * @return String in the form <code>keyGroupName.keyName=value</code>
	 */
	@Override
	public String toString() {
		return key.toString() + "=" + value;
	}

	/**
	 * Gets textual representation of this instance only the name for representing key of the counter.
	 * @return String in the form <code>keyName=value</code>
	 */
	public String toStringShort() {
		return getKeyName() + "=" + value;
	}

	/**
	 * Gets textual representation of this instance using display name for representing key of the counter.
	 * @return String in the form <code>keyDisplayName=value</code>
	 */
	public String toStringShortUseDisplayName() {
		return getKeyDisplayName() + "=" + value;
	}
}
