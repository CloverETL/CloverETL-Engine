package org.jetel.util.protocols.sftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Vector;

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
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz) (c) Javlin
 *         Consulting (www.javlinconsulting.cz)
 */
public class SFTPConnection extends URLConnection {

	private static final JSch jsch = new JSch();
	
	protected Session session;
	protected ChannelSftp channel;

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
	 * Changes directory.
	 * 
	 * @return
	 * @throws IOException
	 */
	public void cd(String path) throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			channel.cd(path);
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
		}
	}
	
	@Override
	public void connect() throws IOException {
		String[] user = getUserInfo();
		try {
			connect(new URLUserInfo(user.length == 2 ? user[1] : null));
		} catch (Exception e) {
			connect(new URLUserInfoIteractive(user.length == 2 ? user[1] : null));
		}
	}

	private void connect(AUserInfo aUserInfo) throws IOException {
		if (session != null && session.isConnected()) return;
		
		String[] user = getUserInfo();
		try {
			if (url.getPort() == 0) session = jsch.getSession(user[0], url.getHost());
			else session = jsch.getSession(user[0], url.getHost(), url.getPort() == -1 ? 22 : url.getPort());

			// password will be given via UserInfo interface.
			session.setUserInfo(aUserInfo);
			session.connect();
		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}

	/**
	 * Session disconnect.
	 */
	public void disconnect() {
		if (session != null && session.isConnected())
			session.disconnect();
	}

	/**
	 * Gets file from remote host.
	 * 
	 * @param remore -
	 *            remote path
	 * @param os -
	 *            output stream
	 * @throws IOException
	 */
	public void get(String remore, OutputStream os) throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			channel.get(remore, os);
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
		}
	}

	@Override
	public InputStream getInputStream() throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			String file = url.getFile();
			InputStream is = new SFTPInputStream(session, channel.get(file.equals("") ? "/" : file));
			return is;
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
			channel = getChannelSftp();
			return new SFTPOutputStream(session, channel.put(url.getFile(), mode));
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		}
	}

	/**
	 * Lists path.
	 * 
	 * @return
	 * @throws IOException
	 */
	public Vector ls(String path) throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			return channel.ls(path);
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
		}
	}

	/**
	 * Pwd command.
	 * 
	 * @return
	 * @throws IOException
	 */
	public String pwd() throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			return channel.pwd();
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} finally {
		}
	}

	/**
	 * Supports sftp put mode that can be ChannelSftp.APPEND or
	 * ChannelSftp.OVERWRITE or ChannelSftp.RESUME value.
	 * 
	 * @param mode
	 */
	public void setMode(int mode) {
		this.mode = mode;
	}

	public void setTimeout(int timeout) throws JSchException {
		session.setTimeout(timeout);
	}

	/**
	 * Gets informations for actual path.
	 * 
	 * @return
	 * @throws IOException
	 */
	public SftpATTRS stat(String path) throws IOException {
		connect();
		try {
			channel = getChannelSftp();
			return channel.stat(path);
		} catch (JSchException e) {
			throw new IOException(e.getMessage());
		} catch (SftpException e) {
			throw new IOException(e.getMessage());
		} finally {
		}
	}

	private String[] getUserInfo() {
		String userInfo;
		return (userInfo = url.getUserInfo()) == null ? new String[] { "" }
				: userInfo.split(":");
	}

	/**
	 * Gets ChannelSftp.
	 * 
	 * @return ChannelSftp
	 * @throws JSchException
	 */
	private ChannelSftp getChannelSftp() throws JSchException {
		if (channel == null || !channel.isConnected()) {
			channel = (ChannelSftp) session.openChannel(url.getProtocol());
			channel.connect();
		}
		return channel;
	}

	public static abstract class AUserInfo implements UserInfo {
		protected String password;

		protected String passphrase = null;

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

		public String getPassphrase() {
			return passphrase;
		}
	}

	/**
	 * Class for password supporting.
	 * 
	 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz) (c) Javlin
	 *         Consulting (www.javlinconsulting.cz)
	 */
	public static class URLUserInfoIteractive extends AUserInfo implements
			UIKeyboardInteractive {

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

		public String getPassword() {
			return password;
		}

		public boolean promptPassword(String message) {
			return password != null;
		}

	}

	public static void main(String[] s) {
		/*try {
			FileUtils.getReadableChannel(null, "gzip:(sftp://jausperger:relatko5@linuxweb:/home/jausperger/public_html/employees0.dat.gz)");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/
		
		try {
			//URL.setURLStreamHandlerFactory(new CloverURLStreamHandlerFactory());
			
			// URL("sftp://e-potrisalova:W8MPJYLm@eft.boehringer-ingelheim.com:/LSC/AED/Export/O_*");
			// URL("sftp://e-potrisalova:W8MPJYLm@eft.boehringer-ingelheim.com:/LSC/AED/Export/O_010_0000000000017003.xml");
			URL url = new URL("sftp://jausperger:relatko5@linuxweb:/home/jausperger/public_html/");
			/*URL url = new URL("sftp://jausperger:relatko5@home.javlinconsulting.cz:/home/jausperger/public_html/");*/
			SFTPConnection con = (SFTPConnection) url.openConnection();
			// con.getInputStream();

			con.connect();
			System.out.println(con.pwd());
			con.cd("/home/jausperger/public_html/xml");
			con.ls(con.pwd());
			System.out.println(con.pwd());
			
			/*Vector v = con.ls(con.pwd());
			Iterator it = v.iterator();
			LsEntry entry;
			while (it.hasNext()) {
				entry = (LsEntry) it.next();
				con.get("/LSC/AED/Export/" + entry.getFilename(), System.out);
			}
			con.disconnect();*/

		} catch (Throwable e) {
			System.err.println(e.getMessage());
		}
		System.exit(0);
	}

}
