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
package org.jetel.metadata;

import org.jetel.util.string.StringUtils;

/**
 * Container type of data field - SINGLE, LIST and MAP.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17 Jan 2012
 */
public enum DataFieldContainerType {

	SINGLE("", (byte) 0), LIST("list", (byte) 1), MAP("map", (byte) 2);
	
	private final String displayName;
	
	/** This byte identifier is used by metadata serialisation into a byte stream.
	 * (This is used by CloverDataReader/Writer to serialise used metadata into data file.)
	 * DataFieldContainerType.ordinal() could be used instead, but this could be non-intentionally
	 * changed by adding new container type. So custom ordinal number is used instead to ensure
	 * stability against code changes. */
	private byte byteIdentifier;

	private DataFieldContainerType(String displayName, byte byteIdentifier) {
		this.displayName = displayName;
		this.byteIdentifier = byteIdentifier;
	}
	
	/**
	 * @param strType
	 * @return container type based on string without case sensitivity
	 */
	public static DataFieldContainerType fromString(String strType) {
		if (StringUtils.isEmpty(strType)) {
			return SINGLE;
		}
		for (DataFieldContainerType type : values()) {
			if (type.name().equalsIgnoreCase(strType)) {
				return type;
			}
		}
		
		throw new IllegalArgumentException("Uknown type of data field container '" + strType + "'.");
	}

	@Override
	public String toString() {
		return getDisplayName();
	}
	
	/**
	 * @return the display name of the container type
	 */
	public String getDisplayName() {
		return displayName;
	}
	
	/** This byte identifier is used by metadata serialisation into a byte stream.
	 * (This is used by CloverDataReader/Writer to serialise used metadata into data file.)
	 * DataFieldContainerType.ordinal() could be used instead, but this could be non-intentionally
	 * changed by adding new container type. So custom ordinal number is used instead to ensure
	 * stability against code changes. */
	public byte getByteIdentifier() {
		return byteIdentifier;
	}
	
	/**
	 * Returns an array containing all display names.
	 * @return
	 */
	public static String[] getDisplayNames() {
		DataFieldContainerType[] values = values();
		String[] result = new String[values.length];
		for (int i = 0; i < values.length; i++) {
			result[i] = values[i].displayName;
		}
		return result;
	}
	
	/**
	 * @param byteIdentifier
	 * @return container type associated with given byte identifier
	 * @see #getByteIdentifier()
	 */
	public static DataFieldContainerType fromByteIdentifier(byte byteIdentifier) {
		for (DataFieldContainerType containerType : values()) {
			if (containerType.getByteIdentifier() == byteIdentifier) {
				return containerType;
			}
		}
		
		throw new IllegalArgumentException("Unknown field container type identifier '" + byteIdentifier + "'.");
	}

}
