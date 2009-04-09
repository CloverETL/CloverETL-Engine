package org.jetel.util.protocols.proxy;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 * URLHandler for proxy.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class ProxyHandler extends URLStreamHandler {

	@Override
	public URLConnection openConnection(URL url) throws IOException {
		throw new UnsupportedOperationException("The opentConnection method is not supported for the ProxyHandler!");
	}
	
    protected void parseURL(URL u, String spec, int start, int limit) {
    	super.parseURL(u, spec, start, limit);
    	String protocol = u.getProtocol();
    	if (ProxyProtocolEnum.fromString(protocol) == null) {
    		throw new RuntimeException("Parse error: The URL protocol have to be one of " + ProxyProtocolEnum.values());
    	}
    }

}
