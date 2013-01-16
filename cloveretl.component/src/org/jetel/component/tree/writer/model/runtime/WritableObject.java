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
import org.jetel.component.tree.writer.model.design.ObjectNode;
import org.jetel.component.tree.writer.model.runtime.WritableMapping.MappingWriteState;
import org.jetel.data.DataRecord;
import org.jetel.data.ListDataField;
import org.jetel.exception.JetelException;

/**
 * Class representing xml element / JSON object / Java object...
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 20 Dec 2010
 */
public class WritableObject extends WritableContainer {

	private final boolean hidden;
	private final boolean root;

	public WritableObject(WritableValue name, WritableValue prefix, boolean writeNull, boolean root) {
		this(name, prefix, writeNull, null, ObjectNode.HIDE_DEFAULT, root);
	}
	
	public WritableObject(WritableValue name, WritableValue prefix, boolean writeNull, boolean hidden, boolean root) {
		this(name, prefix, writeNull, null, hidden, root);
	}

	public WritableObject(WritableValue name, WritableValue prefix, boolean writeNull, PortBinding portBinding, boolean hidden, boolean root) {
		super(name, prefix, writeNull, portBinding);
		this.hidden = hidden;
		this.root = root;
	}

	@Override
	public void writeContent(TreeFormatter formatter, DataRecord[] availableData) throws JetelException, IOException {
		if (root) {
			for (Writable child : children) {
				child.write(formatter, availableData);
			}
		} else if (!isNodeEmpty(availableData)) {
			/*
			 * if list is not supported (XML) and value is empty,
			 * do not write anything
			 */
			if (!formatter.isListSupported() && isEmptyListElement(availableData)) {
				return;
			}
			MappingWriteState state = formatter.getMapping().getState();
			char[] nodeName = name.getValue(availableData);
			if (!hidden && (state == MappingWriteState.ALL || state == MappingWriteState.HEADER)) {
				formatter.getTreeWriter().writeStartNode(nodeName);
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
			if (!hidden && (state == MappingWriteState.ALL || state == MappingWriteState.HEADER)) {
				formatter.getTreeWriter().writeEndNode(nodeName, writeNull);
			}
		}
	}
	
	/**
	 * Answers whether this object writes a list that is empty - 
	 * such list should not produce any elements
	 * @return
	 */
	private boolean isEmptyListElement(DataRecord availableData[]) {
		
		if (children.length == 1 && children[0] instanceof WritableValue) {
			WritableValue value = (WritableValue)children[0];
			if (value.isValuesList()) {
				ListDataField field = (ListDataField)value.getContent(availableData);
				return field.getValue() == null || field.getValue().isEmpty();
			}
		} 
		return false;
	}

	@Override
	public boolean isNodeEmpty(DataRecord[] availableData) {
		if (namespaces.length > 0) {
			return false;
		}
		if (!super.isNodeEmpty(availableData)) {
			return false;
		}
		for (Writable attribute : attributes) {
			if (!attribute.isEmpty(availableData)) {
				return false;
			}
		}

		return true;
	}
}
