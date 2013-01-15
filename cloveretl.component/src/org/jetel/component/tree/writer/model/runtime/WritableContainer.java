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
package org.jetel.component.tree.writer.model.runtime;

import java.io.IOException;

import org.jetel.component.tree.writer.TreeFormatter;
import org.jetel.component.tree.writer.model.runtime.WritableMapping.MappingWriteState;
import org.jetel.data.DataRecord;
import org.jetel.exception.JetelException;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 9.1.2012
 */
public abstract class WritableContainer implements Writable {

	private PortBinding portBinding;

	protected final DynamicName name;
	protected final boolean writeNull;

	protected Writable[] children = new Writable[0];
	protected WritableNamespace[] namespaces = new WritableNamespace[0];
	protected WritableAttribute[] attributes = new WritableAttribute[0];

	public WritableContainer(WritableValue name, WritableValue prefix, boolean writeNull, PortBinding portBinding) {
		this.name = new DynamicName(name, prefix);
		this.writeNull = writeNull;
		
		this.portBinding = portBinding;
		if (this.portBinding != null) {
			this.portBinding.setContainer(this);
		}
	}

	@Override
	public final void write(TreeFormatter formatter, DataRecord[] availableData) throws JetelException,
			IOException {
		if (portBinding != null) {
			portBinding.write(formatter, availableData);
		} else {
			WritableMapping mapping = formatter.getMapping();
			MappingWriteState state = mapping.getState();
		
			if (state == MappingWriteState.ALL || state == MappingWriteState.HEADER) {
				writeContainerStart(formatter, availableData);
			} 

			writeContent(formatter, availableData);
			
			state = mapping.getState();
			if (state == MappingWriteState.ALL || state == MappingWriteState.FOOTER) {
				writeContainerEnd(formatter, availableData);
			}
		}
	}

	@Override
	public final boolean isEmpty(DataRecord[] availableData) {
		if (portBinding != null) {
			return false;
		}

		return isNodeEmpty(availableData);
	}

	public void addChild(Writable element) {
		Writable[] newArray = new Writable[children.length + 1];
		System.arraycopy(children, 0, newArray, 0, children.length);
		newArray[children.length] = element;
		children = newArray;
		if (element instanceof WritableValue) {
			WritableValue value = (WritableValue)element;
			value.setParentContainer(this);
		}
	}

	public void addAttribute(WritableAttribute element) {
		WritableAttribute[] newArray = new WritableAttribute[attributes.length + 1];
		System.arraycopy(attributes, 0, newArray, 0, attributes.length);
		newArray[attributes.length] = element;
		attributes = newArray;
	}

	public void addNamespace(WritableNamespace element) {
		WritableNamespace[] newArray = new WritableNamespace[namespaces.length + 1];
		System.arraycopy(namespaces, 0, newArray, 0, namespaces.length);
		newArray[namespaces.length] = element;
		namespaces = newArray;
	}

	public PortBinding getPortBinding() {
		return portBinding;
	}

	public abstract void writeContent(TreeFormatter formatter, DataRecord[] availableData)
			throws JetelException, IOException;

	protected boolean isNodeEmpty(DataRecord[] availableData) {
		if (writeNull) {
			return false;
		}
		for (Writable child : children) {
			if (!child.isEmpty(availableData)) {
				return false;
			}
		}

		return true;
	}

	public void writeContainerStart(TreeFormatter formatter, DataRecord[] availableData) throws JetelException{
	}

	public void writeContainerEnd(TreeFormatter formatter, DataRecord[] availableData) throws JetelException {
	}
	
}
