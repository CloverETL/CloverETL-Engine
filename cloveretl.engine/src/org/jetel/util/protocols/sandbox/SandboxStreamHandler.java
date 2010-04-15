package org.jetel.util.protocols.sandbox;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import org.jetel.graph.TransformationGraph;

/**
 * 
 * @author mvarecha
 *
 */
public class SandboxStreamHandler extends URLStreamHandler {

	public static final String SANDBOX_PROTOCOL = "sandbox";
	public static final String SANDBOX_PROTOCOL_URL_PREFIX = SANDBOX_PROTOCOL+"://";
	
	private final TransformationGraph graph; 
	
	public SandboxStreamHandler(TransformationGraph graph) {
		super();
		this.graph = graph;
	}

	/*
	 * (non-Javadoc)
	 * @see java.net.URLStreamHandler#openConnection(java.net.URL)
	 */
	@Override
	public URLConnection openConnection(URL url) throws IOException {
		return new SandboxConnection(graph, url);
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.net.URLStreamHandler#parseURL(java.net.URL, java.lang.String, int, int)
	 */
    protected void parseURL(URL u, String spec, int start, int limit) {
    	super.parseURL(u, spec, start, limit);
    	String protocol = u.getProtocol();
    	if (!protocol.equals(SANDBOX_PROTOCOL)) {
    		throw new RuntimeException("Parse error: The URL protocol name must be sandbox!");
    	} else {
    		this.getClass(); // just to have a line for break-point
    	}
    }

}
