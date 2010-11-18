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
package org.jetel.util.protocols.sandbox;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.IAuthorityProxy;

public class SandboxConnection extends URLConnection {
	
	private static final Log log = LogFactory.getLog(SandboxConnection.class);
	private final TransformationGraph graph;

	/**
	 * SFTP constructor.
	 * @param graph 
	 * 
	 * @param url
	 */
	protected SandboxConnection(TransformationGraph graph, URL url) {
		super(url);
		this.graph = graph;
	}

	/*
	 * (non-Javadoc)
	 * @see java.net.URLConnection#getInputStream()
	 */
	@Override
	public InputStream getInputStream() throws IOException {
		String storageCode = url.getHost();
		String path = url.getPath();
		if (graph != null) {
			long runId = graph.getRuntimeContext().getRunId();
			return graph.getAuthorityProxy().getSandboxResourceInput(runId, storageCode, path);
		} else {
			return IAuthorityProxy.getDefaultProxy().getSandboxResourceInput(0, storageCode, path);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.net.URLConnection#getOutputStream()
	 */
	@Override
	public OutputStream getOutputStream() throws IOException {
		String storageCode = url.getHost();
		String path = url.getPath();
		if (graph != null) {
			long runId = graph.getRuntimeContext().getRunId();
			return graph.getAuthorityProxy().getSandboxResourceOutput(runId, storageCode, path);
		} else {
			return IAuthorityProxy.getDefaultProxy().getSandboxResourceOutput(0, storageCode, path);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.net.URLConnection#connect()
	 */
	@Override
	public void connect() throws IOException {
	}

}
