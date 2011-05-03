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

	BIG_ENDIAN(ByteOrder.BIG_ENDIAN),
	LITTLE_ENDIAN(ByteOrder.LITTLE_ENDIAN),
	PACKED_DECIMAL(null);
	
	public static final String BINARY_FORMAT_PREFIX = "BINARY:";

	public final ByteOrder byteOrder;
	
	private BinaryFormat(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}
	
	public String getFormatString() {
		return BINARY_FORMAT_PREFIX + this;
	}
}

