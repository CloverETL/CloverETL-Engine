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

import org.jetel.util.string.StringUtils;

/**
 * <p>Represents Hadoop map/reduce job counter. Internal Hadoop API key of the every counter is an {@link Enum}
 * constant. Instances of this interface provide unambiguous identification of that constant by providing fully
 * qualified name of enum class declaring the constant (see {@link #getGroupName()}) and declared name of the constant
 * itself ({@link #getName()}).</p>
 * 
 * <p>This class represents only a counter key containing all information needed to obtain the value of the counter, not
 * the actual value.</p>
 * 
 * <p>Equality relation is defined on instances of this class by overriding methods {@link Object#hashCode()} and
 * {@link Object#equals(Object)}. Two instances are equal if and only if they have equal counter name a and group
 * name.</p>
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 14.12.2012
 * @see HadoopCounterGroup
 * @see HadoopJobReporter#getCounterValue(HadoopCounterKey)
 */
public class HadoopCounterKey {

	private String name;
	private String groupName;
	private String displayName;

	/**
	 * Creates new {@code HadoopCounterKey} having given name and group name.
	 * @param name A non-null nor empty name of the counter. By contract, it must be string equal to declared name of
	 *        Hadoop API specific {@code Enum} constant that is used as key for the counter by Hadoop API.
	 * @param groupName A non-null nor empty name of the counters group that counter represented by new instance belongs
	 *        to. By contract it is name of the {@code Enum} class that declares coresponding Hadoop API specific key
	 *        for the counter.
	 */
	public HadoopCounterKey(String name, String groupName) {
		this(name, groupName, null);
	}

	/**
	 * Creates new {@code HadoopCounterKey} for given name, group name and user front-end name.
	 * @param name A non-null nor empty name of the counter. By contract, it must be string equal to declared name of
	 *        Hadoop API specific {@code Enum} constant that is used as key for the counter by Hadoop API.
	 * @param groupName A non-null nor empty name of the counters group that counter represented by new instance belongs
	 *        to. By contract it is fully qualified name of the {@code Enum} class that declares corresponding Hadoop
	 *        API specific key for the counter.
	 * @param displayName User front end name meant to be displayed to users.
	 */
	public HadoopCounterKey(String name, String groupName, String displayName) {
		if (name == null) {
			throw new NullPointerException("name");
		}
		if (groupName == null) {
			throw new NullPointerException("counterGroup");
		}
		if (name.isEmpty()) {
			throw new IllegalArgumentException("name cannot be empty.");
		}
		if (groupName.isEmpty()) {
			throw new IllegalArgumentException("groupName cannot be empty.");
		}

		this.name = name;
		this.groupName = groupName;
		this.displayName = displayName;
	}

	/**
	 * Gets name of the counter. By contract, it must be string equal to declared name of Hadoop API specific
	 * {@code Enum} constant that is used as key for the counter by Hadoop API.
	 * @return Name of the counter. It is granted to be non-null nor empty string.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Gets name of the group this counter belongs to. By contract it is fully qualified name of the {@code Enum} class
	 * that declares corresponding Hadoop API specific key of the counter. This group name is not intended to be display
	 * to user.
	 * @return Name of the counters group that this counter belongs to. It is granted to be non-null nor empty string.
	 */
	public String getGroupName() {
		return groupName;
	}

	/**
	 * Gets display name of the counter which is a name intended to be displayed to user.
	 * @return Value of display name if it was provided in constructor, value returned by {@link #getName()} otherwise.
	 */
	public String getDisplayName() {
		return StringUtils.isEmpty(displayName) ? getName() : displayName;
	}

	/**
	 * Sets counter name to be displayed.
	 * @param displayName New value of counter name intended to be displayed to user.
	 */
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	
	/**
	 * Textual representation of this {@code HadoopCounterKey} instance.
	 * @return Group name of the counter followed by dot and name of the counter.
	 */	
	@Override
	public String toString() {
		return groupName + "." + name;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + groupName.hashCode();
		result = prime * result + name.hashCode();
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
		if (!(obj instanceof HadoopCounterKey)) {
			return false;
		}

		HadoopCounterKey other = (HadoopCounterKey) obj;
		return groupName.equals(other.groupName) && name.equals(other.name);
	}
}
