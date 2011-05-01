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

import java.io.IOException;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import org.jetel.component.xml.writer.XmlFormatter;
import org.jetel.component.xml.writer.model.WritableMapping.MappingWriteState;
import org.jetel.data.DataRecord;

/**
 * Class representing xml element
 * 
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20 Dec 2010
 */
public class WritableElement implements Writable {
	
	protected final char[] name;
	protected Writable[] children = new Writable[0];
	protected Writable[] namespaces = new Writable[0];
	protected Writable[] attributes = new Writable[0];
	
	private final boolean writeNull;

	public WritableElement(String name, String prefix, boolean writeNull) {
		
		if (prefix != null && !prefix.isEmpty()) {
			this.name = (prefix + ":" + name).toCharArray();
		} else {
			this.name = name != null ? name.toCharArray() : null;
		}
		this.writeNull = writeNull;
	}

	@Override
	public void write(XmlFormatter formatter, Map<Integer, DataRecord> availableData) throws XMLStreamException, IOException {
		if (name == null) {
			for (Writable child : children) {
				child.write(formatter, availableData);
			}
		} else if (writeNull || !isEmpty(availableData)) {
			MappingWriteState state = formatter.getMapping().getState();
			if (state == MappingWriteState.ALL || state == MappingWriteState.HEADER) {
				if (children.length == 0) {
					formatter.getWriter().writeEmptyElement(name);
				} else {
					formatter.getWriter().writeStartElement(name);
				}
				for (Writable namespace : namespaces) {
					namespace.write(formatter, availableData);
				}
				for (Writable attribute : attributes) {
					attribute.write(formatter, availableData);
				}
			}
			if (state != MappingWriteState.NOTHING) {
				for (Writable child : children) {
					child.write(formatter, availableData);
				}
			}
			state = formatter.getMapping().getState();
			if ((state == MappingWriteState.ALL || state == MappingWriteState.HEADER) && children.length != 0) {
				formatter.getWriter().writeEndElement();
			}
		}
	}

	@Override
	public boolean isEmpty(Map<Integer, DataRecord> availableData) {
		if (namespaces.length == 0) {
			for (Writable child : children) {
				if (!child.isEmpty(availableData)) {
					return false;
				}
			}
			for (Writable attribute : attributes) {
				if (!attribute.isEmpty(availableData)) {
					return false;
				}
			}
			return true;
		}
		return false;
	}
	
	public void addChild(Writable element) {
		Writable[] newArray = new Writable[children.length + 1];
		System.arraycopy(children, 0, newArray, 0, children.length);
		newArray[children.length] = element;
		children = newArray;
	}
	
	public void addAttribute(Writable element) {
		Writable[] newArray = new Writable[attributes.length + 1];
		System.arraycopy(attributes, 0, newArray, 0, attributes.length);
		newArray[attributes.length] = element;
		attributes = newArray;
	}
	
	public void addNamespace(Writable element) {
		Writable[] newArray = new Writable[namespaces.length + 1];
		System.arraycopy(namespaces, 0, newArray, 0, namespaces.length);
		newArray[namespaces.length] = element;
		namespaces = newArray;
	}

	@Override
	public String getText(Map<Integer, DataRecord> availableData) {
		throw new UnsupportedOperationException();
	}
}
