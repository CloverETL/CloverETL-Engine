package org.jetel.util.protocols.sftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.Vector;

import org.jetel.util.protocols.CloverURLStreamHandlerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;
import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * URL Connection for sftp protocol.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class SFTPConnection extends URLConnection {

	protected Session session;
	protected int mode;

	/**
	 * SFTP constructor.
	 * 
	 * @param url
	 */
	protected SFTPConnection(URL url) {
		super(url);
		mode = ChannelSftp.OVERWRITE;
	}

	/**
	 * Supports sftp put mode that can be ChannelSftp.APPEND or ChannelSftp.OVERWRITE or ChannelSftp.RESUME value.
	 * 
	 * @param mode 
	 */
	public void setMode(int mode) {
		this.mode = mode;
	}
	
	@Override
	public InputStream getInputStream() throws IOException {
		connect();
		try {
			ChannelSftp c = getChannelSftp();
			return c.get(url.getFile());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		}
	}

	@Override
    public OutputStream getOutputStream() throws IOException {
		connect();
		try {
			ChannelSftp c = getChannelSftp();
			return c.put(url.getFile(), mode);
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		}
    }

	public void setTimeout(int timeout) throws JSchException {
		session.setTimeout(timeout);
	}
	
	/**
	 * Lists actual path.
	 * 
	 * @return
	 * @throws IOException
	 */
    public Vector ls() throws IOException {
		connect();
		ChannelSftp c = null;
		try {
			c = getChannelSftp();
			Vector v = c.ls(url.getFile());
			return v;
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
			if (c != null && c.isConnected()) c.disconnect();
		}
    }

    /**
     * Gets file from remote host.
     * 
     * @param remore - remote path
     * @param os - output stream
     * @throws IOException
     */
    public void get(String remore, OutputStream os) throws IOException {
		connect();
		ChannelSftp c = null;
		try {
			c = getChannelSftp();
			c.get(remore, os);
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
			if (c != null && c.isConnected()) c.disconnect();
		}
    }

    /**
     * Gets informations for actual path.
     * 
     * @return
     * @throws IOException
     */
    public SftpATTRS stat() throws IOException {
		connect();
		ChannelSftp c = null;
		try {
			c = getChannelSftp();
			return c.stat(url.getFile());
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
			if (c != null && c.isConnected()) c.disconnect();
		}
    }
    
	@Override
	public void connect() throws IOException {
		if (session != null && session.isConnected()) return;
		JSch jsch = new JSch();

		String[] user = url.getUserInfo().split(":");
		try {
			if (url.getPort() == 0) {
				session = jsch.getSession(user[0], url.getHost());
			} else {
				session = jsch.getSession(user[0], url.getHost(), url.getPort() == -1 ? 22 : url.getPort());
			}

			// password will be given via UserInfo interface.
			session.setUserInfo(new URLUserInfo(user.length == 2 ? user[1] : null));

			session.connect();
		} catch (JSchException e) {
			connect2();
		} catch (Exception e) {
			connect2();
		}
	}

	public void disconnect() {
		if (session != null && session.isConnected()) session.disconnect();
	}
	
	private void connect2() throws IOException {
		JSch jsch = new JSch();

		String[] user = url.getUserInfo().split(":");
		try {
			if (url.getPort() == 0) {
				session = jsch.getSession(user[0], url.getHost());
			} else {
				session = jsch.getSession(user[0], url.getHost(), url.getPort() == -1 ? 22 : url.getPort());
			}

			// password will be given via UserInfo interface.
			session.setUserInfo(new URLUserInfoIteractive(user.length == 2 ? user[1] : null));

			session.connect();
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (Exception e) {
			throw new IOException("Couldn't connect to: " + url.toExternalForm() + ": " + e.getMessage());
		}
	}

	/**
	 * Gets ChannelSftp.
	 * 
	 * @return ChannelSftp
	 * @throws JSchException
	 */
    private ChannelSftp getChannelSftp() throws JSchException {
    	ChannelSftp channel = (ChannelSftp) session.openChannel(url.getProtocol());
		channel.connect();
		return channel;
    }
    
    public static abstract class AUserInfo implements UserInfo {
		protected String password;
		protected String passphrase=null;

		public AUserInfo(String password) {
			this.password = password;
		}

		public void showMessage(String message) {
		}
		
		public boolean promptPassphrase(String message) {
			return true;
		}

		public boolean promptYesNo(String str) {
			return true;
		}
		
	    public String getPassphrase(){ 
	    	return passphrase; 
	    }
    }
    
    /**
     * Class for password supporting.
     * 
     * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
	 *         (c) Javlin Consulting (www.javlinconsulting.cz)
     */
	public static class URLUserInfoIteractive extends AUserInfo implements UIKeyboardInteractive {

		public URLUserInfoIteractive(String password) {
			super(password);
		}

		public String getPassword() {
			return null;
		}

		public boolean promptPassword(String message) {
			return true;
		}

		public String[] promptKeyboardInteractive(String destination,
				String name, String instruction, String[] prompt, boolean[] echo) {
			return true ? new String[] { password } : null;
		}
	}
	public static class URLUserInfo extends AUserInfo {

		public URLUserInfo(String password) {
			super(password);
		}

	    public String getPassword(){  
	    	return password; 
	    }
	    
	    public boolean promptPassword(String message){
	        return password!=null;
	    }	      
	      
	}

	public static void main(String [] s) {
        try {
			URL.setURLStreamHandlerFactory(new CloverURLStreamHandlerFactory());
			URL url = new URL("sftp://e-potrisalova:W8MPJYLm@eft.boehringer-ingelheim.com:/LSC/AED/Export/O_*");
			//URL url = new URL("sftp://e-potrisalova:W8MPJYLm@eft.boehringer-ingelheim.com:/LSC/AED/Export/O_010_0000000000017003.xml");
			//URL url = new URL("sftp://jausperger:relatko5@linuxweb:/home/jausperger/public_html/xml/*");
			//URL url = new URL("sftp://jausperger:relatko5@home.javlinconsulting.cz:/home/jausperger/public_html/*");
			SFTPConnection con = (SFTPConnection) url.openConnection();
			//con.getInputStream();
			
			Vector v = con.ls();
			Iterator it = v.iterator();
			LsEntry entry;
			while (it.hasNext()) {
				entry = (LsEntry) it.next();
				con.get("/LSC/AED/Export/" + entry.getFilename(), System.out);
			}
			con.disconnect();
			
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		System.exit(0);
	}
	
}
