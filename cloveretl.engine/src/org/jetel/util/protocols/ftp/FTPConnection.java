/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.util.protocols.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.jetel.util.protocols.UserInfo;

/**
 * URL Connection for sftp protocol.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz) (c) Javlin
 *         Consulting (www.javlinconsulting.cz)
 */
public class FTPConnection extends URLConnection {
	
	private static final Log log = LogFactory.getLog(FTPConnection.class);

	private FTPClient ftp;
	
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

		UserInfo userInfo = UserInfo.fromURL(url);
		ftp.connect(url.getHost(), 21);
		if(!ftp.login(userInfo.getUser(), userInfo.getPassword())) {
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
}
