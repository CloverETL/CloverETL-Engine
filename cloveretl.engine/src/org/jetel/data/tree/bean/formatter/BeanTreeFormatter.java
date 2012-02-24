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

import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.formatter.CollectionWriter;
import org.jetel.data.tree.formatter.NamespaceWriter;
import org.jetel.data.tree.formatter.TreeFormatter;
import org.jetel.data.tree.formatter.TreeWriter;
import org.jetel.data.tree.formatter.runtimemodel.WritableMapping;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.11.2011
 */
public class BeanTreeFormatter extends TreeFormatter {
	
	private BeanWriter writer;
	
	private SchemaObject structure;
	private ClassLoader classloader;
	
	private Collection<Object> target;

	public BeanTreeFormatter(WritableMapping mapping, int maxPortIndex, SchemaObject structure, ClassLoader classloader) {
		super(mapping, maxPortIndex);
		this.structure = structure;
		this.classloader = classloader;
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
		writer = new BeanWriter(structure, classloader);
	}

	@Override
	public void flush() throws IOException {
		Object result = writer.flushBean();
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
	
	// current bean mapping validation detail
	@Override
	public NamespaceWriter getNamespaceWriter() {
		return writer;
	}
}
