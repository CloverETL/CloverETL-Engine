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

import static com.hierynomus.msfscc.fileinformation.FileBasicInformation.DONT_SET;
import static org.jetel.component.fileoperation.SMB2Utils.getPath;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.jetel.component.fileoperation.pool.ConnectionPool;
import org.jetel.component.fileoperation.pool.PooledSMB2Connection;
import org.jetel.component.fileoperation.pool.SMB2Authority;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.stream.StreamUtils;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msdtyp.FileTime;
import com.hierynomus.mserref.NtStatus;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileBasicInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.smbj.common.SMBApiException;
import com.hierynomus.smbj.share.DiskEntry;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17. 7. 2017
 */
public class PrimitiveSMB2OperationHandler implements RecursiveDeleteHandler {
	
	private ConnectionPool pool = ConnectionPool.getInstance();
	
	private PooledSMB2Connection getConnection(URI uri) throws IOException {
		try {
			return (PooledSMB2Connection) pool.borrowObject(new SMB2Authority(uri));
		} catch (Exception e) {
			throw ExceptionUtils.getIOException(e);
		}
	}
	
	@Override
	public boolean createFile(URI target) throws IOException {
    	try (PooledSMB2Connection connection = getConnection(target)) {
    		try (Closeable file = SMB2Utils.openFile(connection.getShare(), getPath(target), EnumSet.of(AccessMask.FILE_ADD_FILE), SMB2CreateDisposition.FILE_OPEN_IF)) {
    		}
    		return true;
    	} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
    	}
	}

	/**
	 * @see FileAttributes
	 */
	@Override
	public boolean setLastModified(URI target, Date date) throws IOException {
    	try (PooledSMB2Connection connection = getConnection(target)) {
    		try (DiskEntry file = SMB2Utils.open(connection.getShare(), getPath(target), EnumSet.of(AccessMask.FILE_WRITE_ATTRIBUTES), SMB2CreateDisposition.FILE_OPEN)) {
    			FileTime changeTime = FileTime.fromDate(date);
				FileBasicInformation newMetadata = new FileBasicInformation(DONT_SET, DONT_SET, changeTime, DONT_SET, 0); // 0x00000000 means no change for file attributes 
				file.setFileInformation(newMetadata);
    		}
    		return true;
    	} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
    	}
	}

	@Override
	public boolean makeDir(URI target) throws IOException {
    	try (PooledSMB2Connection connection = getConnection(target)) {
    		connection.getShare().mkdir(getPath(target));
    		return true;
    	} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
    	}
	}

	@Override
	public boolean deleteFile(URI target) throws IOException {
    	try (PooledSMB2Connection connection = getConnection(target)) {
    		connection.getShare().rm(getPath(target));
    		return true;
    	} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
    	}
	}

	@Override
	public boolean removeDir(URI target) throws IOException {
    	return removeDir(target, false);
	}

	@Override
	public boolean removeDirRecursively(URI target) throws IOException {
    	return removeDir(target, true);
	}
	
	private boolean removeDir(URI target, boolean recursive) throws IOException {
		try (PooledSMB2Connection connection = getConnection(target)) {
    		connection.getShare().rmdir(getPath(target), recursive);
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
		return null; // does not seem to be supported
	}

	@Override
	public ReadableByteChannel read(URI source) throws IOException {
		InputStream is = getInputStream(source);
		return Channels.newChannel(is);
	}

	@Override
	public WritableByteChannel write(URI target) throws IOException {
		return write(target, false);
	}

	@Override
	public WritableByteChannel append(URI target) throws IOException {
		return write(target, true);
	}

	private WritableByteChannel write(URI target, boolean append) throws IOException {
		OutputStream os = getOutputStream(target, append);
		return Channels.newChannel(os);
	}

	private OutputStream getOutputStream(URI target) throws IOException {
		return getOutputStream(target, false);
	}
	
	private InputStream getInputStream(URI source) throws IOException {
		PooledSMB2Connection connection = getConnection(source);
		return SMB2Utils.getInputStream(connection, source);
	}
	
	private OutputStream getOutputStream(URI target, boolean append) throws IOException {
		PooledSMB2Connection connection = getConnection(target);
		return SMB2Utils.getOutputStream(connection, target, append);
	}

    @Override
	public Info info(URI target) throws IOException {
    	try (PooledSMB2Connection connection = getConnection(target)) {
    		String path = getPath(target);
    		try {
    			FileAllInformation file = connection.getShare().getFileInformation(path);
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
    	return list(target, null, false);
	}
	
	private boolean isDirectory(FileIdBothDirectoryInformation file) {
		return SMB2Utils.hasFlag(file.getFileAttributes(), FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue());
	}
	
	public List<URI> list(URI base, String mask, boolean dirsOnly) throws IOException {
    	try (PooledSMB2Connection connection = getConnection(base)) {
    		String path = getPath(base);
    		List<FileIdBothDirectoryInformation> children = connection.getShare().list(path, mask);
    		List<URI> result = new ArrayList<>(children.size());
    		for (FileIdBothDirectoryInformation child: children) {
    			if (dirsOnly && !isDirectory(child)) {
    				continue;
    			}
    			String fileName = child.getFileName();
    			if (!fileName.equals(URIUtils.CURRENT_DIR_NAME) && !fileName.equals(URIUtils.PARENT_DIR_NAME)) {
    				result.add(URIUtils.getChildURI(base, fileName));
    			}
    		}
    		
    		return result;
    	} catch (SMBApiException ex) {
    		// this weird error is thrown if there are no files that match the mask
    		if ((mask != null) && (ex.getStatus() == NtStatus.UNKNOWN)) {
    			return Collections.emptyList();
    		}
    		throw ExceptionUtils.getIOException(ex);
    	} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
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
			return hasAttribute(FileAttributes.FILE_ATTRIBUTE_HIDDEN);
		}
		
		private boolean hasAttribute(FileAttributes attribute) {
			return SMB2Utils.hasFlag(file.getBasicInformation().getFileAttributes(), attribute.getValue());
		}
		
		private boolean hasAccess(AccessMask accessMask) {
			return SMB2Utils.hasFlag(file.getAccessInformation().getAccessFlags(), accessMask.getValue());
		}

		@Override
		public Boolean canRead() {
			return hasAccess(AccessMask.FILE_READ_DATA);
		}

		@Override
		public Boolean canWrite() {
			return hasAccess(AccessMask.FILE_WRITE_DATA);
		}

		@Override
		public Boolean canExecute() {
			return hasAccess(AccessMask.FILE_EXECUTE);
		}

		@Override
		public Type getType() {
			return isDirectory() ? Type.DIR : Type.FILE;
		}

		@Override
		public Date getLastModified() {
			return file.getBasicInformation().getLastWriteTime().toDate();
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
