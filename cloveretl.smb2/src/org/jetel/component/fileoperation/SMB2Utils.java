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
package org.jetel.component.fileoperation;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import org.jetel.component.fileoperation.pool.PooledSMB2Connection;
import org.jetel.plugin.Plugins;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.stream.CloseOnceInputStream;
import org.jetel.util.stream.CloseOnceOutputStream;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileDirectoryInformation;
import com.hierynomus.msfscc.fileinformation.FileStandardInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.smbj.share.DiskEntry;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;

public class SMB2Utils {

	private static final String PLUGIN_ID = "org.jetel.smb2";

	public static ClassLoader getClassLoader() {
		return Plugins.getPluginDescriptor(PLUGIN_ID).getClassLoader();
	}
	
	public static String getPath(URI uri) {
		return new SMB2Path(uri).getPath();
	}
	
	public static String getPath(URL url) {
		return new SMB2Path(url).getPath();
	}
	
	public static boolean hasFlag(long flags, long mask) {
		return (flags & mask) != 0;
	}
	
	public static boolean isDirectory(FileDirectoryInformation file) {
		return isDirectory(file.getFileAttributes());
	}
	
	public static boolean isDirectory(long fileAttributes) {
		return SMB2Utils.hasFlag(fileAttributes, FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue());
	}
	
	static com.hierynomus.smbj.share.File openFile(DiskShare share, String path, Set<AccessMask> accessMask, SMB2CreateDisposition createDisposition) throws IOException {
		// use SMB2ShareAccess.ALL to prevent TimeoutException / buffer underflow on concurrent operations
		return share.openFile(path, accessMask, null, SMB2ShareAccess.ALL, createDisposition, null);
	}
	
	static DiskEntry open(DiskShare share, String path, Set<AccessMask> accessMask, SMB2CreateDisposition createDisposition) throws IOException {
		// use SMB2ShareAccess.ALL to prevent TimeoutException / buffer underflow on concurrent operations
		return share.open(path, accessMask, null, SMB2ShareAccess.ALL, createDisposition, null);
	}
	
	public static InputStream getInputStream(PooledSMB2Connection connection, URL source) throws IOException {
		return getInputStream(connection, getPath(source));
	}
	
	public static OutputStream getOutputStream(PooledSMB2Connection connection, URL target, boolean append) throws IOException {
		return getOutputStream(connection, getPath(target), append);
	}

	public static InputStream getInputStream(PooledSMB2Connection connection, URI source) throws IOException {
		return getInputStream(connection, getPath(source));
	}
	
	public static OutputStream getOutputStream(PooledSMB2Connection connection, URI target, boolean append) throws IOException {
		return getOutputStream(connection, getPath(target), append);
	}

	private static InputStream getInputStream(final PooledSMB2Connection connection, String path) throws IOException {
		try {
			DiskShare share = connection.getShare();
			
			Set<AccessMask> accessMask = EnumSet.of(AccessMask.FILE_READ_DATA);
			SMB2CreateDisposition createDisposition = SMB2CreateDisposition.FILE_OPEN;
			
			final com.hierynomus.smbj.share.File file = openFile(share, path, accessMask, createDisposition);
			InputStream is = file.getInputStream();
			is = new CloseOnceInputStream(is) {

				@Override
				protected void doClose() throws IOException {
					try {
						super.doClose();
					} finally {
						FileUtils.closeAll(file, connection);
					}
				}
				
			};
			return is;
		} catch (Throwable t) {
			connection.returnToPool();
			throw ExceptionUtils.getIOException(t);
		}
	}

	private static OutputStream getOutputStream(final PooledSMB2Connection connection, String path, boolean append) throws IOException {
		try {
			DiskShare share = connection.getShare();
			
			Set<AccessMask> accessMask = append ? EnumSet.of(AccessMask.FILE_APPEND_DATA) : EnumSet.of(AccessMask.FILE_WRITE_DATA);
			SMB2CreateDisposition createDisposition = append ? SMB2CreateDisposition.FILE_OPEN_IF : SMB2CreateDisposition.FILE_OVERWRITE_IF;
			
			final com.hierynomus.smbj.share.File file = openFile(share, path, accessMask, createDisposition);
			OutputStream os = append ? new AppendOutputStream(file) : file.getOutputStream();
			if (append) { // add buffering, AppendOutputStream is slow
				os = new BufferedOutputStream(os, connection.getWriteBufferSize());
			}
			os = new CloseOnceOutputStream(os) {

				@Override
				protected void doClose() throws IOException {
					try {
						super.doClose();
					} finally {
						FileUtils.closeAll(file, connection);
					}
				}
				
			};
			return os;
		} catch (Throwable t) {
			connection.returnToPool();
			throw ExceptionUtils.getIOException(t);
		}
	}

	/**
	 * Utility {@link OutputStream} for appending.
	 * Does not close the parent {@link com.hierynomus.smbj.share.File}.
	 * 
	 * @author krivanekm
	 *
	 * @see com.hierynomus.smbj.share.FileOutputStream
	 */
	private static class AppendOutputStream extends OutputStream {
		
		private final File file;
		
		private long fileOffset;
		private final byte[] temp = new byte[1];
		
		private boolean closed;
		
		public AppendOutputStream(File file) {
			this.file = Objects.requireNonNull(file);
			this.fileOffset = file.getFileInformation(FileStandardInformation.class).getEndOfFile();
		}

		@Override
		public void write(int b) throws IOException {
			temp[0] = (byte)b;
			write(temp, 0, 1);
		}

		@Override
		public void write(byte[] b) throws IOException {
			write(b, 0, b.length);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			verifyConnectionNotClosed();
			int total = 0;
			while (total < len) {
				int count = file.write(b, fileOffset, off + total, len - total);
				total += count;
				fileOffset += count;
			}
		}

	    private void verifyConnectionNotClosed() throws IOException {
	        if (closed) {
	        	throw new IOException("Stream is closed");
	        }
	    }

		// flushing does not seem to be necessary and file.flush() doesn't seem to work
		@Override
		public void flush() throws IOException {
			verifyConnectionNotClosed();
			// file.flush(); // causes STATUS_USER_SESSION_DELETED error
		}

		/**
		 * Does not close the parent {@link File}.
		 */
		@Override
		public void close() throws IOException {
			closed = true;
			// file.close() will be performed by the wrapping DelegateOutputStream above
		}

	}

}
