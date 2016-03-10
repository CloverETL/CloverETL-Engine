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

import org.jetel.component.RecordsFilter;
import org.jetel.component.tree.writer.TreeFormatter;
import org.jetel.component.tree.writer.model.design.ObjectNode;
import org.jetel.component.tree.writer.model.runtime.WritableMapping.MappingWriteState;
import org.jetel.component.tree.writer.util.WriteNullElement;
import org.jetel.data.DataRecord;
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
	protected final String dataType;
	protected final boolean rawValue;

	public WritableObject(WritableValue name, WritableValue prefix, WriteNullElement writeNull, boolean root, boolean rawValue, String dataType) {
		this(name, prefix, writeNull, null, ObjectNode.HIDE_DEFAULT, root, rawValue, dataType); 
	}
	
	public WritableObject(WritableValue name, WritableValue prefix, WriteNullElement writeNull, boolean hidden, boolean root, boolean rawValue, String dataType) {
		this(name, prefix, writeNull, null, hidden, root, rawValue, dataType);
	}

	public WritableObject(WritableValue name, WritableValue prefix, WriteNullElement writeNull, PortBinding portBinding, boolean hidden, boolean root, boolean rawValue, String dataType) {
		super(name, prefix, writeNull, portBinding);
		this.hidden = hidden;
		this.root = root;
		this.dataType = dataType;
		this.rawValue = rawValue;
	}

	@Override
	public void writeContent(TreeFormatter formatter, DataRecord[] availableData) throws JetelException, IOException {
		if (root) {
			for (Writable child : children) {
				child.write(formatter, availableData);
			}
		} else if (!isNodeEmpty(formatter, availableData)) {
			if (getPortBinding() != null) {
				// This allows filtering records on the "top" level. CLO-4852
				// The "top" level means partitionElement of WritableMapping, not the xml root.
				RecordsFilter recordFilter = getPortBinding().getRecordFilter();
				if (recordFilter != null && !recordFilter.isValid(availableData)) {
					// record didn't pass through filter, just skip it
					return;
				}
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
	
	@Override
	public void addChild(WritableValue value) {
		super.addChild(value);
		value.dataType = dataType;
		value.rawValue = rawValue;
	}

	@Override
	public boolean isNodeEmpty(TreeFormatter formatter, DataRecord[] availableData) {
		/* namespaces alone do not count - 
		 * something need to be actually written
		if (namespaces.length > 0) {
			return false;
		}
		*/
		if (!super.isNodeEmpty(formatter, availableData)) {
			return false;
		}
		if (writeNull == WriteNullElement.FALSE_EXCLUDE_IF_INNER_CONTENT_IS_NULL) {
			return true; // CLO-4852
		}
		
		// check attributes
		for (Writable attribute : attributes) {
			if (!attribute.isEmpty(formatter, availableData)) {
				return false;
			}
		}

		return true;
	}
}
