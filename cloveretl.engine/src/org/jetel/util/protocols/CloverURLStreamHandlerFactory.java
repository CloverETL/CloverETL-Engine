package org.jetel.util.protocols;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

import org.jetel.util.protocols.sftp.SFTPStreamHandler;

/**
 * Simple URLStreamHandlerFactory for sftp and scp protocol.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class CloverURLStreamHandlerFactory implements URLStreamHandlerFactory {

	/**
	 * @see java.net.URLStreamHandlerFactory.createURLStreamHandler
	 */
	public URLStreamHandler createURLStreamHandler(String protocol) {
		if (protocol.equals("sftp") || protocol.equals("scp")) {
			return new SFTPStreamHandler();
		} 
		return null;
	}

}
