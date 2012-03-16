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

import org.jetel.component.tree.writer.TreeFormatter;
import org.jetel.component.tree.writer.model.runtime.WritableMapping.MappingWriteState;
import org.jetel.data.DataRecord;
import org.jetel.exception.JetelException;

/**
 * class representing xml text node, can be used as a value of attribute or element.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 20 Dec 2010
 */
public abstract class WritableValue implements Writable {

	public static WritableValue newInstance(NodeValue[] value) {
		if (value == null) {
			throw new NullPointerException("value");
		}
		if (value.length == 1) {
			return new WritableSimpleValue(value[0]);
		} else {
			return new WritableComplexValue(value);
		}
	}

	@Override
	public void write(TreeFormatter formatter, DataRecord[] availableData) throws JetelException {
		MappingWriteState state = formatter.getMapping().getState();
		if (state == MappingWriteState.ALL || state == MappingWriteState.HEADER) {
			formatter.getTreeWriter().writeLeaf(getContent(availableData));
		}
	}

	@Override
	public abstract boolean isEmpty(DataRecord[] availableData);

	public abstract Object getContent(DataRecord[] availableData);

	private static class WritableComplexValue extends WritableValue {

		private final NodeValue[] value;

		WritableComplexValue(NodeValue[] value) {
			this.value = value;
		}

		@Override
		public boolean isEmpty(DataRecord[] availableData) {
			for (NodeValue element : value) {
				if (!element.isEmpty(availableData)) {
					return false;
				}
			}
			return true;
		}

		@Override
		public String getContent(DataRecord[] availableData) {
			StringBuilder builder = new StringBuilder();
			for (NodeValue part : value) {
				builder.append(part.getValue(availableData));
			}
			return builder.toString();
		}
	}

	private static class WritableSimpleValue extends WritableValue {

		private final NodeValue value;

		WritableSimpleValue(NodeValue value) {
			this.value = value;
		}

		@Override
		public Object getContent(DataRecord[] availableData) {
			return value.getValue(availableData);
		}

		@Override
		public boolean isEmpty(DataRecord[] availableData) {
			return value.isEmpty(availableData);
		}
	}

}
