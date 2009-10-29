package org.jetel.util.protocols.sandbox;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.TransformationGraph;

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
		long runId = graph.getRuntimeContext().getRunId();
		InputStream is = graph.getAuthorityProxy().getSandboxResourceInput(runId, storageCode, path);
		return is;
	}

	/*
	 * (non-Javadoc)
	 * @see java.net.URLConnection#getOutputStream()
	 */
	@Override
	public OutputStream getOutputStream() throws IOException {
		String storageCode = url.getHost();
		String path = url.getPath();
		long runId = graph.getRuntimeContext().getRunId();
		OutputStream os = graph.getAuthorityProxy().getSandboxResourceOutput(runId, storageCode, path);
		return os;
	}

	/*
	 * (non-Javadoc)
	 * @see java.net.URLConnection#connect()
	 */
	@Override
	public void connect() throws IOException {
	}

}
