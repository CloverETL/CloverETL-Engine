package org.jetel.util.protocols.sftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
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

	/**
	 * Lists actual path.
	 * 
	 * @return
	 * @throws IOException
	 */
    public Vector ls() throws IOException {
		connect();
		try {
			ChannelSftp c = getChannelSftp();
			return c.ls(url.getFile());
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
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
		try {
			ChannelSftp c = getChannelSftp();
			c.get(remore, os);
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
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
		try {
			ChannelSftp c = getChannelSftp();
			return c.stat(url.getFile());
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		}
    }
    
	@Override
	public void connect() throws IOException {
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
			throw new IOException(e.getMessage());
		} catch (Exception e) {
			connect2();
			throw new IOException("Couldn't connect to: " + url.toExternalForm() + ": " + e.getMessage());
		}
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
			session.setUserInfo(new URLUserInfo(user.length == 2 ? user[1] : null));

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
    
    /**
     * Class for password supporting.
     * 
     * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
	 *         (c) Javlin Consulting (www.javlinconsulting.cz)
     */
	public static class URLUserInfoIteractive implements UserInfo, UIKeyboardInteractive {

		private String password;

		public URLUserInfoIteractive(String password) {
			this.password = password;
		}

		public String getPassword() {
			return null;
		}

		public boolean promptYesNo(String str) {
			return true;
		}

		public String getPassphrase() {
			return null;
		}

		public boolean promptPassphrase(String message) {
			return true;
		}

		public boolean promptPassword(String message) {
			return true;
		}

		public void showMessage(String message) {
		}

		public String[] promptKeyboardInteractive(String destination,
				String name, String instruction, String[] prompt, boolean[] echo) {
			return true ? new String[] { password } : null;
		}
	}
	public static class URLUserInfo implements UserInfo {

	    public boolean promptYesNo(String str){
	        return true;
	    }

	    private String passwd=null;
	    private String passphrase=null;

	    public String getPassword(){  return passwd; }
	    public String getPassphrase(){ return passphrase; }

	    public boolean promptPassword(String message){
	        return passwd!=null;
	    }
	      
	    public boolean promptPassphrase(String message){
	        return true;
	    }
	      
	    public void showMessage(String message){
	    }

		public URLUserInfo(String password) {
			this.passwd = password;
		}
	}

	public static void main(String [] s) {
        try {
			URL.setURLStreamHandlerFactory(new CloverURLStreamHandlerFactory());
			URL url = new URL("sftp://e-potrisalova:W8MPJYLm@eft.boehringer-ingelheim.com:/LSC/AED/Export/*");
			//URL url = new URL("sftp://jausperger:relatko5@linuxweb:/home/jausperger/public_html/xml/*");
			//URL url = new URL("sftp://jausperger:relatko5@home.javlinconsulting.cz:/home/jausperger/public_html/*");
			URLConnection con = url.openConnection();
			con.getInputStream();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
	}
	
}
