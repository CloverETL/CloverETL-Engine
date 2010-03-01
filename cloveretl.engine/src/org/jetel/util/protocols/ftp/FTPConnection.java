package org.jetel.util.protocols.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

/**
 * URL Connection for sftp protocol.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz) (c) Javlin
 *         Consulting (www.javlinconsulting.cz)
 */
public class FTPConnection extends URLConnection {
	
	private static final Log log = LogFactory.getLog(FTPConnection.class);

	private FTPClient ftp;
	
	// standard encoding for URLDecoder
	// see http://www.w3.org/TR/html40/appendix/notes.html#non-ascii-chars
	private static final String ENCODING = "UTF-8";

	/**
	 * SFTP constructor.
	 * 
	 * @param url
	 */
	protected FTPConnection(URL url) {
		super(url);
		ftp = new FTPClient();
	}

	/**
	 * Changes directory.
	 * 
	 * @return
	 * @throws IOException
	 */
	public boolean cd(String path) throws IOException {
		connect();
		return ftp.changeWorkingDirectory(path);
	}
	
	@Override
	public void connect() throws IOException {
		if (ftp.isConnected()) return;
		try {
			ftp.disconnect();
		} catch(Exception e) {
//			log.warn("error closing ftp connection", e);
		}

		String[] user = getUserInfo();
		ftp.connect(url.getHost(), 21);
		if(!ftp.login(user.length >= 1 ? user[0] : "", user.length >= 2 ? user[1] : "")) {
            ftp.logout();
            throw new IOException("Authentication failed.");
        }
		ftp.enterLocalPassiveMode();
		
		int reply = ftp.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply))
        	disconnect();
	}

	/**
	 * Session disconnect.
	 * @throws IOException 
	 */
	public void disconnect() throws IOException {
		if (ftp != null && ftp.isConnected())
            ftp.disconnect();
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
	/*public void get(String remore, OutputStream os) throws IOException {
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
	}*/

	@Override
	public InputStream getInputStream() throws IOException {
		connect();
		return ftp.retrieveFileStream(url.getFile());
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		connect();
		return ftp.storeFileStream(url.getFile());
	}

	private String[] getUserInfo() {
		String userInfo = url.getUserInfo();
		if (userInfo == null) return new String[] {""};
		return decodeString(userInfo).split(":");
	}

	/**
	 * Decodes string.
	 * @param s
	 * @return
	 */
	private String decodeString(String s) {
		try {
			return URLDecoder.decode(s, ENCODING);
		} catch (UnsupportedEncodingException e) {
			return s;
		}
	}
	
	/**
	 * Lists path.
	 * 
	 * @return
	 * @throws IOException
	 */
	public FTPFile[] ls(String path) throws IOException {
		connect();
		return ftp.listFiles(path);
	}

	/**
	 * Pwd command.
	 * 
	 * @return
	 * @throws IOException
	 */
	public String pwd() throws IOException {
		connect();
		return ftp.printWorkingDirectory();
	}

	public void setSoTimeout(int timeout) throws SocketException {
		ftp.setSoTimeout(timeout);
	}


	public static void main(String[] s) {
		try {
			//ReadableByteChannel rc = FileUtils.getReadableChannel(null, "ftp://jausperger:relatko5@linuxweb:21/public_html/g.html");
			URL url = new URL(null, "ftp://jausperger:relatko5@linuxweb:21/public_html/", new FTPStreamHandler());
			FTPConnection con = (FTPConnection) url.openConnection();
			// con.getInputStream();

			con.connect();
			
			System.out.println(con.pwd());
			con.cd("public_html");
			//con.ls(con.pwd());
			System.out.println(con.pwd());
			
			con.ls("path");

			//files[10].getName()
			con.ftp.listFiles("orders.dat")[0].isDirectory();
			con.ftp.listFiles("xml");
			
			con.disconnect();

		} catch (Throwable e) {
			log.error("error",e);
		}
		System.exit(0);
	}

}
