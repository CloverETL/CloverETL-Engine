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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.jetel.util.string.StringUtils;

/**
 * <p>Represents named and ordered collection of {@link HadoopCounterKey} instances. It corresponds to Hadoop API specific
 * counters group that consists of all counters that are identified by {@link Enum} constants of the same enumeration.
 * This class provides abstraction from this Hadoop API specific enumerations.</p>
 * 
 * <p>Instances of this class are immutable. Equality relation is defined on instances of this class by overriding
 * methods {@link Object#hashCode()} and {@link Object#equals(Object)}. Two instances are equal if and only if they have
 * equal group name.</p>
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 * @see HadoopCounterKey
 */
public class HadoopCounterGroup extends AbstractCollection<HadoopCounterKey> {

	private String groupName;
	private String groupDisplayName;
	private List<HadoopCounterKey> counters;

	/**
	 * Initializes new counter group using given list of counters, group name and group display name.
	 * @param counters Non-null nor empty collection of counters that new counters group will include. By contract, all
	 *        the counters keys in this collection must have their <code>counterGroupName</code> attribute set to string
	 *        equal to name of this group.
	 * @param groupName Unique, non-null group name for this group. By contract, this must be string equal to class name
	 *        of corresponding Hadoop API specific counters enumeration that is represented by constructed instance.
	 * @param groupDisplayName Display name of the counters group intended to be displayed to user or <code>null</code>.
	 * @see HadoopCounterKey#getGroupName()
	 */
	public HadoopCounterGroup(Collection<HadoopCounterKey> counters, String groupName, String groupDisplayName) {
		if (counters == null) {
			throw new NullPointerException("hadoopCounterKeys");
		}
		if (counters.isEmpty()) {
			throw new IllegalArgumentException("Empty counter groups with no counters are not allowed.");
		}
		if (groupName == null) {
			throw new NullPointerException("groupName");
		}
		if (groupName.isEmpty()) {
			throw new IllegalArgumentException("groupName cannot be empty string.");
		}
		for (HadoopCounterKey c : counters) {
			if (!groupName.equals(c.getGroupName())) {
				throw new IllegalArgumentException("Counter group name of counter " + c + " is not equal to name"
						+ " of the counters group it is beeing added to. The name of this group was: " + groupName);
			}
		}
		this.groupName = groupName;
		this.groupDisplayName = groupDisplayName;
		this.counters = new ArrayList<HadoopCounterKey>(counters);
	}

	/**
	 * Automatically creates {@link HadoopCounterKey} instances for enum constants representing individual counters and
	 * initializes new counter group with them. A name of the Enum class is used as both group name and group name of
	 * created counters.
	 * @param hadoopApiCounterKeys Non-null nor empty array of enum constants that represents Hadoop API counters keys
	 *        that are to be used to create {@link HadoopCounterKey}s of this group.
	 * @param groupDisplayName Display name of the counters group intended to be displayed to user.
	 * @param countersDisplayNameGetter Is used to initialize display names of individual {@link HadoopCounterKey}s when
	 *        creating them. When null, display names are not created.
	 */
	public <T extends Enum<T>> HadoopCounterGroup(T[] hadoopApiCounterKeys, String groupDisplayName,
			HadoopCounterDisplayNameGetter<T> countersDisplayNameGetter) {
		this(enumsToCounters(hadoopApiCounterKeys, countersDisplayNameGetter), hadoopApiCounterKeys[0].getClass()
				.getName(), groupDisplayName);
	}

	/**
	 * Automatically creates {@link HadoopCounterKey} instances for enum constants representing individual counters and
	 * initializes new counter group with them. A name of the Enum class is used as both group nameand group name of
	 * created counters.
	 * @param hadoopCounterKeys Non-null nor empty array of enum constants that represents Hadoop API counters keys that
	 *        are to be used to create {@link HadoopCounterKey}s of this group.
	 * @param groupDisplayName Display name of the counters group intended to be displayed to user.
	 */
	public <T extends Enum<T>> HadoopCounterGroup(T[] hadoopCounterKeys, String displayName) {
		this(hadoopCounterKeys, displayName, null);
	}

	/**
	 * Automatically creates {@link HadoopCounterKey} instances for enum constants representing individual counters and
	 * initializes new counter group with them. A name of the Enum class is used as group name, group display name and
	 * group name of created counters.
	 * @param hadoopCounterKeys Non-null nor empty array of enum constants that represents Hadoop API counters keys that
	 *        are to be used to create {@link HadoopCounterKey}s of this group.
	 */
	public <T extends Enum<T>> HadoopCounterGroup(T[] hadoopCounterKeys) {
		this(hadoopCounterKeys, null);
	}

	/**
	 * Wraps given enums and creates {@link HadoopCounterKey}s for each enum constant.
	 * @param hadoopCounterKeys Enum constants to be wrapped.
	 * @param countersDisplayNameGetter Operation used to create display names for returned counters. If null no display
	 *        names are created.
	 * @return Created <code>HadoopCounter</code>s.
	 */
	private static <T extends Enum<T>> List<HadoopCounterKey> enumsToCounters(T[] hadoopCounterKeys,
			HadoopCounterDisplayNameGetter<T> countersDisplayNameGetter) {
		if (hadoopCounterKeys == null) {
			throw new NullPointerException("hadoopCounterKeys");
		}
		if (hadoopCounterKeys.length == 0) {
			throw new IllegalArgumentException("hadoopCounterKeys array may not be empty");
		}
		List<HadoopCounterKey> result = new ArrayList<HadoopCounterKey>();
		for (T ck : hadoopCounterKeys) {
			result.add(new HadoopCounterKey(ck.name(), ck.getClass().getName(),
					countersDisplayNameGetter == null ? null : countersDisplayNameGetter.getDisplayNameForKey(ck)));
		}
		return result;
	}

	/**
	 * Gets group name. By contract, this must be string equal to class name of corresponding Hadoop API specific
	 * counters enumeration that is represented by this {@code HadoopCounterGroup}.
	 * @return
	 */
	public String getGroupName() {
		return groupName;
	}

	/**
	 * Gets display name of the counter group. If it was not specified at creation time, was <code>null</code> or empty
	 * a value returned by {@link #getGroupName()} is used instead.
	 * @return A name of this counter group that is intended to be displayed to user.
	 */
	public String getGroupDisplayName() {
		return StringUtils.isEmpty(groupDisplayName) ? getGroupName() : groupDisplayName;
	}

	@Override
	public Iterator<HadoopCounterKey> iterator() {
		return Collections.unmodifiableList(counters).iterator();
	}

	@Override
	public int hashCode() {
		return groupName.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof HadoopCounterGroup)) {
			return false;
		}
		HadoopCounterGroup other = (HadoopCounterGroup) obj;
		return groupName.equals(other.groupName);
	}

	@Override
	public int size() {
		return counters.size();
	}
}
