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

import java.nio.ByteOrder;

import org.jetel.util.string.StringUtils;

/**
 * Enumeration of formats that may be used when reading
 * (multibyte) binary data.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 28.4.2011
 */
public enum BinaryFormat {

	BIG_ENDIAN("Big-endian, variable length", ByteOrder.BIG_ENDIAN),
	LITTLE_ENDIAN("Little-endian, variable length", ByteOrder.LITTLE_ENDIAN),
	PACKED_DECIMAL("Packed decimal, variable length. May be used with implicit decimal point."),
	DOUBLE_BIG_ENDIAN("Double, big-endian (8 bytes)", ByteOrder.BIG_ENDIAN, 8),
	DOUBLE_LITTLE_ENDIAN("Double, little-endian (8 bytes)", ByteOrder.LITTLE_ENDIAN, 8),
	FLOAT_BIG_ENDIAN("Float, big-endian (4 bytes)", ByteOrder.BIG_ENDIAN, 4),
	FLOAT_LITTLE_ENDIAN("Float, little-endian (4 bytes)", ByteOrder.LITTLE_ENDIAN, 4);
	
	public static final String BINARY_FORMAT_PREFIX = "BINARY:";

	public final String description;
	public final ByteOrder byteOrder;
	public final Integer size;
	
	private BinaryFormat(String description) {
		this(description, null);
	}
	
	private BinaryFormat(String description, ByteOrder byteOrder) {
		this(description, byteOrder, null);
	}
	
	private BinaryFormat(String description, ByteOrder byteOrder, Integer size) {
		this.description = description;
		this.byteOrder = byteOrder;
		this.size = size;
	}
	
	public String getFormatString() {
		return BINARY_FORMAT_PREFIX + this;
	}
	
	public static boolean isBinaryFormat(String formatStr) {
		if(!StringUtils.isEmpty(formatStr)) {
			if(formatStr.toUpperCase().startsWith(BINARY_FORMAT_PREFIX.toUpperCase())) {
				return true; 
			}
		}
		return false;
	}

	public static String getBinaryFormatParams(String formatStr) {
		if(!StringUtils.isEmpty(formatStr) && formatStr.startsWith(BINARY_FORMAT_PREFIX)) {
			return formatStr.substring(BINARY_FORMAT_PREFIX.length());
		}
		return "";
	}
	
	public static BinaryFormat fromFormatStr(String formatStr) {
		return valueOf(getBinaryFormatParams(formatStr));
	}
}

