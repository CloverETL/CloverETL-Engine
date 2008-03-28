package org.jetel.util.protocols.sftp;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 * URLStreamHandler for sftp connection.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class SFTPStreamHandler extends URLStreamHandler {

	@Override
	public URLConnection openConnection(URL url) throws IOException {
		return new SFTPConnection(url);
	}
	
    protected void parseURL(URL u, String spec, int start, int limit) {
    	super.parseURL(u, spec, start, limit);
    	String protocol = u.getProtocol();
    	if (!(protocol.equals("sftp") || protocol.equals("scp"))) {
    		throw new RuntimeException("Parse error: The URL protocol must be sftp or scp!");
    	}
    }

}
