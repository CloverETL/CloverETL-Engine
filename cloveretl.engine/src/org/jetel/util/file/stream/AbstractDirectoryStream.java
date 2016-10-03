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
package org.jetel.util.file.stream;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.DirectoryStream;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.GraphRuntimeContext;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5. 9. 2016
 */
public abstract class AbstractDirectoryStream<T> implements DirectoryStream<T>, Iterator<T> {
	
	private boolean iteratorReturned = false;

	private volatile boolean closed = false;
	
	private String origin;

	private static final Log defaultLogger = LogFactory.getLog(AbstractDirectoryStream.class);
	
	/**
	 * 
	 */
	protected AbstractDirectoryStream() {
		try {
			Node node = ContextProvider.getNode();
			this.origin = "";
			if (node != null) {
				this.origin = node.getId();
				TransformationGraph graph = node.getGraph();
				if (graph != null) {
					GraphRuntimeContext context = node.getGraph().getRuntimeContext();
					if (context != null) {
						this.origin += " from " + node.getGraph().getRuntimeContext().getJobUrl();
					}
				}
			}
			try (
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
			) {
				new Exception().printStackTrace(pw);
				pw.close();
				this.origin += sw.toString();
			}
		} catch (Exception ex) {
			defaultLogger.error("", ex);
		}
	}

	@Override
	public void close() throws IOException {
		closed = true;
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (!closed) {
			defaultLogger.error("Resource leak: " + getClass().getSimpleName() + " created by " + origin + " was not closed");
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<T> iterator() {
		if (iteratorReturned || closed) {
			throw new IllegalStateException();
		}
		iteratorReturned = true;
		return this;
	}

}
