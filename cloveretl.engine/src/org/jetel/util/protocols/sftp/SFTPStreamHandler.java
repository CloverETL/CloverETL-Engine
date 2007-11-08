package org.jetel.util.protocols.sftp;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import org.jetel.util.protocols.sftp.SFTPConnection;

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
}
