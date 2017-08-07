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

import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.stream.DelegatingOutputStream;
import org.jetel.util.stream.StreamUtils;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.mserref.NtStatus;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.common.SMBApiException;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17. 7. 2017
 */
public class PrimitiveSMB2OperationHandler implements PrimitiveOperationHandler {
	
	private SMBClient client = new SMBClient();
	
	private Session getSession(URI uri) throws IOException {
		Connection connection = openConnection(uri);
		return startSession(connection, uri);
	}

	private Session startSession(Connection connection, URI uri) throws IOException {
		String userInfoString = uri.getUserInfo();
		String[] userInfo = userInfoString.split(":");
		String username = userInfo[0];
		String password = userInfo[1];
		String domain = null;
		if (username.contains(";")) {
			String[] user = username.split(";");
			domain = user[0];
			username = user[1];
		}
		
		AuthenticationContext authContext = new AuthenticationContext(username, password.toCharArray(), domain);
		return connection.authenticate(authContext);
	}

	private Connection openConnection(URI uri) throws IOException {
		int port = uri.getPort();
		Connection connection;
		if (port > -1) {
			connection = client.connect(uri.getHost(), uri.getPort());
		} else {
			connection = client.connect(uri.getHost());
		}
		return connection;
	}
	
	private SMBPath getPath(URI uri) {
		return new SMBPath(uri);
	}
	
	private DiskShare getShare(Session session, URI uri) throws IOException {
		SMBPath path = getPath(uri);
		return (DiskShare) session.connectShare(path.share);
	}

	@Override
	public boolean createFile(URI target) throws IOException {
    	try (
    		Connection connection = openConnection(target);
    		Session session = startSession(connection, target);
    		DiskShare share = getShare(session, target);
    	) {
    		SMBPath smbPath = getPath(target);
    		try (Closeable file = share.openFile(smbPath.getPath(), EnumSet.of(AccessMask.FILE_ADD_FILE), null, null, null, null)) {
    		}
    		return true;
    	} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
    	}
	}

	@Override
	public boolean setLastModified(URI target, Date date) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean makeDir(URI target) throws IOException {
    	try (
    		Connection connection = openConnection(target);
    		Session session = startSession(connection, target);
    		DiskShare share = getShare(session, target);
    	) {
    		SMBPath smbPath = getPath(target);
    		share.mkdir(smbPath.getPath());
    		return true;
    	} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
    	}
	}

	@Override
	public boolean deleteFile(URI target) throws IOException {
    	try (
    		Connection connection = openConnection(target);
    		Session session = startSession(connection, target);
    		DiskShare share = getShare(session, target);
    	) {
    		SMBPath smbPath = getPath(target);
    		share.rm(smbPath.getPath());
    		return true;
    	} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
    	}
	}

	@Override
	public boolean removeDir(URI target) throws IOException {
    	try (
    		Connection connection = openConnection(target);
    		Session session = startSession(connection, target);
    		DiskShare share = getShare(session, target);
    	) {
    		SMBPath smbPath = getPath(target);
    		share.rmdir(smbPath.getPath(), false);
    		return true;
    	} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
    	}
	}

	@Override
	public URI moveFile(URI source, URI target) throws IOException {
		URI result = copyFile(source, target);
		deleteFile(source);
		return result;
	}

	@Override
	public URI copyFile(URI source, URI target) throws IOException {
		try (
			InputStream is = getInputStream(source);
			OutputStream os = getOutputStream(target);
		) {
			StreamUtils.copy(is, os);
		}
		return target;
	}

	@Override
	public URI renameTo(URI source, URI target) throws IOException {
		return null;
	}

	@Override
	public ReadableByteChannel read(URI source) throws IOException {
		InputStream is = getInputStream(source);
		return Channels.newChannel(is);
	}

	private InputStream getInputStream(URI source) throws IOException {
		final com.hierynomus.smbj.share.File file = openFile(source, EnumSet.of(AccessMask.FILE_READ_DATA), SMB2CreateDisposition.FILE_OPEN);
		InputStream is = file.getInputStream();
		is = new FilterInputStream(is) {

			@Override
			public void close() throws IOException {
				try {
					super.close();
				} finally {
					file.close();
				}
			}
			
		};
		return is;
	}

	@Override
	public WritableByteChannel write(URI target) throws IOException {
		return write(target, false);
	}

	@Override
	public WritableByteChannel append(URI target) throws IOException {
		//return write(target, true);
		return null; // TODO
	}

	private WritableByteChannel write(URI target, boolean append) throws IOException {
		OutputStream os = getOutputStream(target, append);
		return Channels.newChannel(os);
	}

	private OutputStream getOutputStream(URI target) throws IOException {
		return getOutputStream(target, false);
	}

	private OutputStream getOutputStream(URI target, boolean append) throws IOException {
		final com.hierynomus.smbj.share.File file = openFile(target, EnumSet.of(AccessMask.FILE_WRITE_DATA), append ? SMB2CreateDisposition.FILE_OPEN_IF : SMB2CreateDisposition.FILE_SUPERSEDE);
		OutputStream os = file.getOutputStream();
		os = new DelegatingOutputStream(os) {

			@Override
			public void close() throws IOException {
				try {
					super.close();
				} finally {
					file.close();
				}
			}
			
		};
		return os;
	}

	private com.hierynomus.smbj.share.File openFile(URI target, Set<AccessMask> accessMask, SMB2CreateDisposition createDisposition) throws IOException {
		Session session = getSession(target);
		DiskShare share = getShare(session, target);
		SMBPath smbPath = getPath(target);
		String path = smbPath.getPath();
		com.hierynomus.smbj.share.File file = share.openFile(path, accessMask, null, null, createDisposition, null);
		return file;
	}

    @Override
	public Info info(URI target) throws IOException {
    	try (
    		Connection connection = openConnection(target);
    		Session session = startSession(connection, target);
    		DiskShare share = getShare(session, target);
    	) {
    		SMBPath smbPath = getPath(target);
    		String path = smbPath.getPath();
    		try {
    			FileAllInformation file = share.getFileInformation(path);
    			return new SmbFileInfo(target, file);
    		} catch (SMBApiException sae) {
                NtStatus status = sae.getStatus();
    			if (status == NtStatus.STATUS_OBJECT_NAME_NOT_FOUND || status == NtStatus.STATUS_OBJECT_PATH_NOT_FOUND) {
                    return null;
                } else {
                    throw new IOException(sae);
                }
    		}
    	} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
    	}
	}
    
	@Override
	public List<URI> list(URI target) throws IOException {
    	try (
    		Connection connection = openConnection(target);
    		Session session = startSession(connection, target);
    		DiskShare share = getShare(session, target);
    	) {
    		SMBPath smbPath = getPath(target);
    		String path = smbPath.getPath();
    		List<FileIdBothDirectoryInformation> children = share.list(path);
    		List<URI> result = new ArrayList<>(children.size());
    		for (FileIdBothDirectoryInformation child: children) {
    			String fileName = child.getFileName();
    			if (!fileName.equals(URIUtils.CURRENT_DIR_NAME) && !fileName.equals(URIUtils.PARENT_DIR_NAME)) {
    				result.add(URIUtils.getChildURI(target, fileName));
    			}
    		}
    		
    		return result;
    	} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
    	}
	}

	private static class SMBPath {
		
		private String share = "";
		private String path = "";
		private String originalPath = "";
		
		public SMBPath(URI uri) {
			String uriPath = uri.getPath();
			if (uriPath != null) {
				if (uriPath.startsWith("/")) {
					uriPath = uriPath.substring(1);
				}
				
				String[] parts = uriPath.split("/", 2);
				if (parts.length > 0) {
					this.share = parts[0];
				}
				if (parts.length > 1) {
					this.originalPath = parts[1];
				}
				
				this.path = originalPath;
				if (this.path.endsWith(URIUtils.PATH_SEPARATOR)) {
					this.path = this.path.substring(0, this.path.length() - 1);
				}
				this.path = this.path.replace('/', '\\');
			}
		}
		
		public String getPath() {
			return this.path;
		}

		@Override
		public String toString() {
			return share + "/" + path;
		}
		
	}
	
	private static class SmbFileInfo implements Info {
		
		private final FileAllInformation file;
		private final URI uri;

		/**
		 * @param file
		 */
		private SmbFileInfo(URI uri, FileAllInformation file) {
			this.uri = uri;
			this.file = file;
		}

		@Override
		public String getName() {
			return FilenameUtils.getName(file.getNameInformation());
		}

		@Override
		public URI getURI() {
			return uri;
		}

		@Override
		public URI getParentDir() {
			return URIUtils.getParentURI(uri);
		}

		@Override
		public boolean isDirectory() {
			return file.getStandardInformation().isDirectory();
		}

		@Override
		public boolean isFile() {
			return !isDirectory();
		}

		@Override
		public Boolean isLink() {
			return null;
		}

		@Override
		public Boolean isHidden() {
			return null;
		}

		@Override
		public Boolean canRead() {
			return null;
		}

		@Override
		public Boolean canWrite() {
			return null;
		}

		@Override
		public Boolean canExecute() {
			return null;
		}

		@Override
		public Type getType() {
			return isDirectory() ? Type.DIR : Type.FILE;
		}

		@Override
		public Date getLastModified() {
			return file.getBasicInformation().getChangeTime().toDate();
		}

		@Override
		public Date getCreated() {
			return file.getBasicInformation().getCreationTime().toDate();
		}

		@Override
		public Date getLastAccessed() {
			return file.getBasicInformation().getLastAccessTime().toDate();
		}

		@Override
		public Long getSize() {
			return file.getStandardInformation().getEndOfFile();
		}

		@Override
		public String toString() {
			return getURI().toString();
		}
		
	}
	
}
