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

import it.sauronsoftware.ftp4j.FTPClient;
import it.sauronsoftware.ftp4j.FTPDataTransferListener;
import it.sauronsoftware.ftp4j.FTPFile;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.string.StringUtils;

/**
 * Little bit crappy wrapper around FTPClient from ftp4j library. This class provides SeekableByteChannel
 * interface for a file in a ftp repository.
 * Be careful with this class, it is pretty experimental.
 * In the future should be reimplemented by apache ftp client, which is more suitable for this task.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 2.12.2009
 */
public class FTPSeekableByteChannel implements SeekableByteChannel {

	private URL url;
	
	private FTPClient ftpClient;

	private ReadableByteChannel inputChannel;
	
	private long fileSize = -1;
	
	private volatile boolean finished;
	
	/**
	 * @throws ComponentNotReadyException 
	 * 
	 */
	public FTPSeekableByteChannel(String fileURL) throws ComponentNotReadyException {
		this(fileURL, 0);
	}
	
	/**
	 * @throws ComponentNotReadyException 
	 * 
	 */
	public FTPSeekableByteChannel(String fileURL, long position) throws ComponentNotReadyException {
		try {
			url = new URL(fileURL);
		} catch (MalformedURLException e) {
			throw new ComponentNotReadyException("Given fileURL '" + fileURL + "' is invalid.", e);
		}

		ftpClient = new FTPClient();
		try {
			if (url.getPort() != -1) {
				ftpClient.connect(url.getHost(), url.getPort());
			} else {
				System.out.println(ftpClient.connect(url.getHost()));
			}
		} catch (Exception e) {
			throw new ComponentNotReadyException("FTP connection failed.", e);
		}
		
		String[] userInfo = getUserInfo(url);
		try {
			ftpClient.login(userInfo[0], userInfo[1]);
		} catch (Exception e) {
			try {
				close();
			} catch (IOException e1) {
				//DO NOTHING
			}
		}
		
		//what about the proxy?
		//ftpClient.setConnector(arg0)
		
		// what about sftp?
		//ftpClient.setSecurity(FTPClient.SECURITY_FTPS); // enables FTPS
		//ftpClient.setSecurity(FTPClient.SECURITY_FTPES); // enables FTPES
		
		//binary data transport is preferred
		ftpClient.setType(FTPClient.TYPE_BINARY);
		
		try {
			connect(position);
		} catch (IOException e) {
			throw new ComponentNotReadyException("Requested file is not available.", e);
		}
	}
	
	private void connect(final long position) throws IOException {
//		try {
			if (inputChannel != null) {
				inputChannel.close();
			}
//			ftpClient.abortCurrentDataTransfer(false);
//		} catch (FTPIllegalReplyException e) {
//			throw new IOException(e);
//		}
		
		if (fileSize == -1) {
			FTPFile[] fileDesc;
			try {
				fileDesc = ftpClient.list(url.getPath());
			} catch (Exception e) {
				throw new IOException(e);
			}
			
			if (fileDesc != null && fileDesc.length == 1) {
				fileSize = fileDesc[0].getSize();
			} else {
				throw new IOException("File size is not available, file does not exist.");
			}
		}

		
		
		
		PipedInputStream pis = new PipedInputStream();
		final PipedOutputStream pos = new PipedOutputStream(pis);
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					finished = false;
					ftpClient.download(url.getPath(), pos, position, new FTPDataTransferListener() {
						
						@Override
						public void transferred(int arg0) {
							// TODO Auto-generated method stub
							
						}
						
						@Override
						public void started() {
							// TODO Auto-generated method stub
							
						}
						
						@Override
						public void failed() {
							// TODO Auto-generated method stub
							
						}
						
						@Override
						public void completed() {
							try {
								pos.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
						}
						
						@Override
						public void aborted() {
							// TODO Auto-generated method stub
							
						}
					});
					finished = true;
				} catch (Exception e) {
					//DO NOTHING
				}
			}
		}).start();

		inputChannel = Channels.newChannel(pis);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.util.bytes.SeekableByteChannel#position()
	 */
	@Override
	public long position() throws IOException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see org.jetel.util.bytes.SeekableByteChannel#position(long)
	 */
	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		connect(newPosition);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.jetel.util.bytes.SeekableByteChannel#size()
	 */
	@Override
	public long size() throws IOException {
		return fileSize;
	}

	/* (non-Javadoc)
	 * @see org.jetel.util.bytes.SeekableByteChannel#truncate(long)
	 */
	@Override
	public SeekableByteChannel truncate(long size) throws IOException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)
	 */
	@Override
	public int read(ByteBuffer dst) throws IOException {
//		try {
			return inputChannel.read(dst);
//		} catch (IOException e) {
//			return -1;
//		}
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#close()
	 */
	@Override
	public void close() throws IOException {
//		if (ftpClient != null /*&& ftpClient.isConnected()*/) {
//			try {
//				ftpClient.disconnect(true);
//			} catch (Exception e) {
//				throw new IOException(e);
//			}
//		}
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#isOpen()
	 */
	@Override
	public boolean isOpen() {
		return true;
		//return ftpClient.isConnected();
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.WritableByteChannel#write(java.nio.ByteBuffer)
	 */
	@Override
	public int write(ByteBuffer src) throws IOException {
		throw new UnsupportedOperationException();
	}

	private String[] getUserInfo(URL url) {
		String userInfo = url.getUserInfo();
		if (StringUtils.isEmpty(userInfo)) {
			return new String[] { "", null };
		}
		String[] result = userInfo.split(":");
		if (result.length == 1) {
			return new String[] { result[0], null };
		}
		return result;
	}

}
