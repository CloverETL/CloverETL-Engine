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
package org.jetel.component.xml.writer.model;

import java.util.Map;

import javax.xml.stream.XMLStreamException;

import org.jetel.component.xml.writer.XmlFormatter;
import org.jetel.data.DataRecord;

/**
 * @author LKREJCI (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20 Dec 2010
 */
public class DynamicValue implements Writable {
	
	private final int port;
	private final int fieldIndex;
	
	public DynamicValue(int port, int fieldIndex) {
		this.port = port;
		this.fieldIndex = fieldIndex;
	}

	@Override
	public void write(XmlFormatter formatter, Map<Integer, DataRecord> availableData) throws XMLStreamException {
		throw new UnsupportedOperationException("Use WritableValue instead!");
	}

	@Override
	public boolean isEmpty(Map<Integer, DataRecord> availableData) {
		return availableData.get(port).getField(fieldIndex).isNull();
	}

	@Override
	public String getText(Map<Integer, DataRecord> availableData) {
		return availableData.get(port).getField(fieldIndex).toString();
	}
}
