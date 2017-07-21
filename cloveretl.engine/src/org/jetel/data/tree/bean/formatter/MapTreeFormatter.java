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
package org.jetel.data.tree.bean.formatter;

import java.io.IOException;
import java.util.Collection;

import org.jetel.data.tree.formatter.CollectionWriter;
import org.jetel.data.tree.formatter.TreeFormatter;
import org.jetel.data.tree.formatter.TreeWriter;
import org.jetel.data.tree.formatter.runtimemodel.WritableMapping;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author krejcil (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1.2.2012
 */
public class MapTreeFormatter extends TreeFormatter {
	
	private MapWriter writer;

	private Collection<Object> target;

	public MapTreeFormatter(WritableMapping mapping, int maxPortIndex) {
		super(mapping, maxPortIndex);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setDataTarget(Object outputDataTarget) throws IOException {
		if (outputDataTarget instanceof Collection<?>){
			target = (Collection<Object>) outputDataTarget;
		} else {
			throw new IllegalArgumentException("Unsupported target type '" + outputDataTarget.getClass().getName()+ "'");
		}
	}

	@Override
	public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
		super.init(metadata);
		writer = new MapWriter();
	}

	@Override
	public void flush() throws IOException {
		Object result = writer.flushResult();
		if (result != null) {
			target.add(result);
		}
	}

	@Override
	public void close() throws IOException {
		if (writer == null) {
			return;
		}
		flush();
		writer = null;	
	}

	@Override
	public TreeWriter getTreeWriter() {
		return writer;
	}

	@Override
	public CollectionWriter getCollectionWriter() {
		return writer;
	}

}
