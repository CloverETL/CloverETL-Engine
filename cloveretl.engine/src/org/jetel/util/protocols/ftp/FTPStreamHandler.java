package org.jetel.util.protocols.ftp;

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
public class FTPStreamHandler extends URLStreamHandler {

	@Override
	public URLConnection openConnection(URL url) throws IOException {
		return new FTPConnection(url);
	}
	
    protected void parseURL(URL u, String spec, int start, int limit) {
    	super.parseURL(u, spec, start, limit);
    	String protocol = u.getProtocol();
    	if (!protocol.equals("ftp")) {
    		throw new RuntimeException("Parse error: The URL protocol name must be ftp!");
    	}
    }

}
