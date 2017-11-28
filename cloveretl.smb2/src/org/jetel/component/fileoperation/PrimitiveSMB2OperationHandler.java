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
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

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
import com.hierynomus.msfscc.fileinformation.FileDirectoryInformation;
import com.hierynomus.msfscc.fileinformation.FileQueryableInformation;
import com.hierynomus.msfscc.fileinformation.FileStandardInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.smbj.share.DiskEntry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17. 7. 2017
 */
public class PrimitiveSMB2OperationHandler implements RecursiveDeleteHandler, WildcardResolutionHandler, FileMetadataHandler {
	
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
	
	private <F extends FileQueryableInformation> F getFileInformation(URI target, Class<F> informationClass) throws IOException {
    	try (PooledSMB2Connection connection = getConnection(target)) {
    		String path = getPath(target);
            return connection.getShare().getFileInformation(path, informationClass);
    	} catch (SMBApiException sae) {
            NtStatus status = sae.getStatus();
			if (status == NtStatus.STATUS_OBJECT_NAME_NOT_FOUND || status == NtStatus.STATUS_OBJECT_PATH_NOT_FOUND) {
                return null;
            } else {
                throw new IOException(sae);
            }
		} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
    	}
	}

    @Override
	public Info info(URI target) throws IOException {
		FileAllInformation file = getFileInformation(target, FileAllInformation.class);
		return (file != null) ? new SmbFileInfo(target, file) : null;
	}
    
	@Override
	public List<URI> list(URI target) throws IOException {
    	return list(target, null, false);
	}
	
	@SuppressFBWarnings("SF_SWITCH_NO_DEFAULT")
	private List<FileDirectoryInformation> list(URI base, String mask) throws IOException {
		List<FileDirectoryInformation> children;
		String path = getPath(base);
		
    	try (PooledSMB2Connection connection = getConnection(base)) {
    		children = connection.getShare().list(path, FileDirectoryInformation.class, mask);
    	} catch (SMBApiException ex) {
    		throw ExceptionUtils.getIOException(ex);
    	} catch (Exception ex) {
    		throw ExceptionUtils.getIOException(ex);
    	}

    	// filter out "." and ".."
		for (Iterator<FileDirectoryInformation> it = children.iterator(); it.hasNext(); ) {
			FileDirectoryInformation child = it.next();
			String fileName = child.getFileName();
			switch (fileName) {
			case URIUtils.CURRENT_DIR_NAME:
			case URIUtils.PARENT_DIR_NAME:
				it.remove();
			}
		}
		return children;
	}
	
	@Override
	public List<URI> list(URI base, String mask, boolean dirsOnly) throws IOException {
		List<FileDirectoryInformation> children = list(base, mask);
		List<URI> result = new ArrayList<>(children.size());
		for (FileDirectoryInformation child: children) {
			if (dirsOnly && !SMB2Utils.isDirectory(child)) {
				continue;
			}
			result.add(URIUtils.getChildURI(base, child.getFileName()));
		}
		
		return result;
	}
	
	@Override
	public List<Info> listFiles(URI base) throws IOException {
		List<FileDirectoryInformation> children = list(base, null);
		List<Info> result = new ArrayList<>(children.size());
		for (FileDirectoryInformation child: children) {
			String fileName = child.getFileName();
			URI childURI = URIUtils.getChildURI(base, fileName);
			result.add(new SimpleSmbFileInfo(childURI, child));
		}
		
		return result;
	}
	
	private static abstract class AbstractSmbInfo implements Info {

		private final URI uri;

		public AbstractSmbInfo(URI uri) {
			this.uri = Objects.requireNonNull(uri);
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
		public boolean isFile() {
			return !isDirectory();
		}

		@SuppressFBWarnings("NP_BOOLEAN_RETURN_NULL")
		@Override
		public Boolean isLink() {
			return null;
		}

		@Override
		public Boolean isHidden() {
			return hasAttribute(FileAttributes.FILE_ATTRIBUTE_HIDDEN);
		}
		
		/**
		 * <code>true</code>, because we have already checked that the resource exists
		 */
		@Override
		public Boolean canRead() {
			return true;
		}

		/**
		 * <code>true</code> if the resource is not marked read-only
		 */
		@Override
		public Boolean canWrite() {
			return !hasAttribute(FileAttributes.FILE_ATTRIBUTE_READONLY);
		}

		@SuppressFBWarnings("NP_BOOLEAN_RETURN_NULL")
		@Override
		public Boolean canExecute() {
			return null;
		}

		@Override
		public Type getType() {
			return isDirectory() ? Type.DIR : Type.FILE;
		}
		
		private boolean hasAttribute(FileAttributes attribute) {
			return SMB2Utils.hasFlag(getFileAttributes(), attribute.getValue());
		}
		
		@Override
		public Date getLastModified() {
			return getLastWriteTime().toDate();
		}

		@Override
		public Date getCreated() {
			return getCreationTime().toDate();
		}

		@Override
		public Date getLastAccessed() {
			return getLastAccessTime().toDate();
		}

		protected abstract long getFileAttributes();
		
		protected abstract FileTime getLastWriteTime();

		protected abstract FileTime getCreationTime();

		protected abstract FileTime getLastAccessTime();

		@Override
		public String toString() {
			return getURI().toString();
		}
		
	}
	
	private static class SmbFileInfo extends AbstractSmbInfo {
		
		private final FileAllInformation file;

		/**
		 * @param file
		 */
		private SmbFileInfo(URI uri, FileAllInformation file) {
			super(uri);
			this.file = Objects.requireNonNull(file);
		}

		@Override
		public String getName() {
			return FilenameUtils.getName(file.getNameInformation());
		}

		@Override
		public boolean isDirectory() {
			return getStandardInformation().isDirectory();
		}

		@Override
		public Long getSize() {
			return getStandardInformation().getEndOfFile();
		}

		@Override
		protected long getFileAttributes() {
			return getBasicInformation().getFileAttributes();
		}

		@Override
		protected FileTime getLastWriteTime() {
			return getBasicInformation().getLastWriteTime();
		}

		@Override
		protected FileTime getCreationTime() {
			return getBasicInformation().getCreationTime();
		}

		@Override
		protected FileTime getLastAccessTime() {
			return getBasicInformation().getLastAccessTime();
		}

		private FileStandardInformation getStandardInformation() {
			return file.getStandardInformation();
		}

		private FileBasicInformation getBasicInformation() {
			return file.getBasicInformation();
		}

	}

	private static class SimpleSmbFileInfo extends AbstractSmbInfo {
		
		private final FileDirectoryInformation file;
		
		/**
		 * @param file
		 */
		private SimpleSmbFileInfo(URI uri, FileDirectoryInformation file) {
			super(uri);
			this.file = Objects.requireNonNull(file);
		}
		
		@Override
		public String getName() {
			return file.getFileName();
		}

		@Override
		public boolean isDirectory() {
			return SMB2Utils.isDirectory(file);
		}

		@Override
		protected FileTime getLastWriteTime() {
			return file.getLastWriteTime();
		}

		@Override
		protected FileTime getCreationTime() {
			return file.getCreationTime();
		}

		@Override
		protected FileTime getLastAccessTime() {
			return file.getLastAccessTime();
		}

		@Override
		public Long getSize() {
			return file.getEndOfFile();
		}

		@Override
		protected long getFileAttributes() {
			return file.getFileAttributes();
		}

	}
	
}
