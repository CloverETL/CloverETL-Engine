package org.jetel.util.protocols.sftp;

import java.io.IOException;
import java.io.OutputStream;

import com.jcraft.jsch.Session;

public class SFTPOutputStream extends OutputStream {

	private OutputStream os;
	
	private Session session;
	
	public SFTPOutputStream(Session session, OutputStream os) {
		this.os = os;
		this.session = session;
	}

	@Override
	public void write(int b) throws IOException {
		os.write(b);
	}
	
    public void close() throws IOException {
    	flush();
    	os.close();
    	session.disconnect();
    }
    
    public void flush() throws IOException {
    	os.flush();
    }

}
