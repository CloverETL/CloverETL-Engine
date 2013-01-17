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
 * @created 10.1.2012
 */
public class WritableCollection extends WritableContainer {
	
	public WritableCollection(String name, String prefix, boolean writeNull) {
		super(name, prefix, writeNull, null);
	}
	
	public WritableCollection(String name, String prefix, boolean writeNull, PortBinding portBinding) {
		super(name, prefix, writeNull, portBinding);
	}

	@Override
	public void writeContent(TreeFormatter formatter, DataRecord[] availableData) throws JetelException, IOException {
		if (!isNodeEmpty(formatter, availableData)) {
			MappingWriteState state = formatter.getMapping().getState();
			if (state != MappingWriteState.NOTHING) {
				for (Writable child : children) {
					child.write(formatter, availableData);
				}
			}
		}
	}

	@Override
	public void writeContainerStart(TreeFormatter formatter) throws JetelException {
		formatter.getCollectionWriter().writeStartCollection(name);
	}

	@Override
	public void writeContainerEnd(TreeFormatter formatter) throws JetelException {
		formatter.getCollectionWriter().writeEndCollection(name);
	}
}
