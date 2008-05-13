package org.jetel.util.protocols.sftp;

import java.io.IOException;
import java.io.InputStream;

import com.jcraft.jsch.Session;

public class SFTPInputStream extends InputStream {

	private InputStream is;
	
	private Session session;
	
	public SFTPInputStream(Session session, InputStream is) {
		this.is = is;
		this.session = session;
	}
	
	@Override
	public int read() throws IOException {
		return is.read();
	}
	
    public void close() throws IOException {
    	is.close();
    	session.disconnect();
    }
	
}
