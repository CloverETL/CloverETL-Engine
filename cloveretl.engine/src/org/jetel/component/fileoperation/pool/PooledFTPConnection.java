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
package org.jetel.component.fileoperation.pool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.jetel.component.fileoperation.FileOperationMessages;

public class PooledFTPConnection extends AbstractPoolableConnection {

	// standard encoding for URLDecoder
	// see http://www.w3.org/TR/html40/appendix/notes.html#non-ascii-chars
	protected static final String ENCODING = "UTF-8";
	
	public enum WriteMode {
		APPEND,
		OVERWRITE
	}

	protected FTPClient ftp;
	
	protected PooledFTPConnection(Authority authority) throws IOException {
		super(authority);
		this.ftp = new FTPClient();
	}

	/**
	 * Decodes string.
	 * @param s
	 * @return
	 */
	protected String decodeString(String s) {
		try {
			return URLDecoder.decode(s, ENCODING);
		} catch (UnsupportedEncodingException e) {
			return s;
		}
	}

	protected String[] getUserInfo() {
		String userInfo = authority.getUserInfo();
		if (userInfo == null) return new String[] {""};
		return decodeString(userInfo).split(":");
	}

	public void connect() throws IOException {
		if (ftp.isConnected()) {
			return;
		}
//		FTPClientConfig config = new FTPClientConfig();
//		config.setServerTimeZoneId("GMT+0");
//		ftp.configure(config);
		ftp.setListHiddenFiles(true);
		String[] user = getUserInfo();
		try {
			int port = authority.getPort();
			if (port < 0) {
				port = 21;
			}
			ftp.connect(authority.getHost(), port);
			if (!ftp.login(user.length >= 1 ? user[0] : "", user.length >= 2 ? user[1] : "")) { //$NON-NLS-1$ //$NON-NLS-2$
	            ftp.logout();
	            throw new IOException(FileOperationMessages.getString("FTPOperationHandler.authentication_failed")); //$NON-NLS-1$
	        }
			ftp.enterLocalPassiveMode();
			
			int reply = ftp.getReplyCode();
	        if (!FTPReply.isPositiveCompletion(reply)) {
	        	throw new IOException(FileOperationMessages.getString("FTPOperationHandler.connection_failed")); //$NON-NLS-1$
	        }
		} catch (IOException ioe) {
			disconnect();
			throw new IOException(FileOperationMessages.getString("FTPOperationHandler.connection_failed"), ioe); //$NON-NLS-1$
		}
	}

	/**
	 * Session disconnect.
	 * 
	 * @throws IOException 
	 */
	public void disconnect() throws IOException {
		if ((ftp != null) && ftp.isConnected()) {
			ftp.logout();
            ftp.disconnect();
		}
	}

	@Override
	public void close() throws IOException {
		disconnect();
	}

	@Override
	public boolean isOpen() {
		if (ftp == null || !ftp.isConnected()) {
			return false;
		}
		try {
			ftp.noop();
			return true;
		} catch (Exception ex) {
			return false;
		}
	}

	public FTPClient getFtpClient() {
		return ftp;
	}
	
	public InputStream getInputStream(String path) throws IOException {
		try {
			InputStream is = ftp.retrieveFileStream(path);
			if (is == null) {
				throw new IOException(ftp.getReplyString());
			}
			int replyCode = ftp.getReplyCode();
			if (!FTPReply.isPositiveIntermediate(replyCode) && !FTPReply.isPositivePreliminary(replyCode)) {
				is.close();
				throw new IOException(ftp.getReplyString());
			}
			is = new BufferedInputStream(is) {
				@Override
				public void close() throws IOException {
					try {
						super.close();
					} finally {
						try {
							if (!ftp.completePendingCommand()) {
								throw new IOException(FileOperationMessages.getString("FTPOperationHandler.failed_to_close_stream")); //$NON-NLS-1$
							}
						} finally {
							returnToPool();
						}
					}
				}
			};
			return is;
		} catch (Exception e) {
			returnToPool();
			throw getIOException(e);
		}
	}
	
	/**
	 * If the stream cannot be opened, the connection is closed
	 * (returned to the pool).
	 * 
	 * @param path
	 * @param mode
	 * @return
	 * @throws IOException
	 */
	public OutputStream getOutputStream(String path, WriteMode mode) throws IOException {
		try {
			OutputStream os = (mode == WriteMode.APPEND) ? ftp.appendFileStream(path) : ftp.storeFileStream(path);
			if (os == null) {
				throw new IOException(ftp.getReplyString());
			}
			int replyCode = ftp.getReplyCode();
			if (!FTPReply.isPositiveIntermediate(replyCode) && !FTPReply.isPositivePreliminary(replyCode)) {
				os.close();
				throw new IOException(ftp.getReplyString());
			}
			os = new BufferedOutputStream(os) {
				@Override
				public void close() throws IOException {
					try {
						super.close();
					} finally {
						try {
							if (!ftp.completePendingCommand()) {
								throw new IOException(FileOperationMessages.getString("FTPOperationHandler.failed_to_close_stream")); //$NON-NLS-1$
							}
						} finally {
							returnToPool();
						}
					}
				}
			};
			return os;
		} catch (Exception e) {
			returnToPool();
			throw getIOException(e);
		}
	}

}
