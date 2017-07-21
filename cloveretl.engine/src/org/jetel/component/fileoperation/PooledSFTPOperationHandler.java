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

import static java.text.MessageFormat.format;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.fileoperation.SimpleParameters.CopyParameters;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.SimpleParameters.FileParameters;
import org.jetel.component.fileoperation.SimpleParameters.InfoParameters;
import org.jetel.component.fileoperation.SimpleParameters.ListParameters;
import org.jetel.component.fileoperation.SimpleParameters.MoveParameters;
import org.jetel.component.fileoperation.SimpleParameters.ReadParameters;
import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;
import org.jetel.component.fileoperation.SimpleParameters.WriteParameters;
import org.jetel.component.fileoperation.pool.Authority;
import org.jetel.component.fileoperation.pool.ConnectionPool;
import org.jetel.component.fileoperation.pool.PooledSFTPConnection;
import org.jetel.component.fileoperation.pool.SFTPAuthority;
import org.jetel.util.string.StringUtils;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

public class PooledSFTPOperationHandler implements IOperationHandler {

	public static final String SFTP_SCHEME = "sftp"; //$NON-NLS-1$
	
	private static final Log log = LogFactory.getLog(PooledSFTPOperationHandler.class);
	
	private FileManager manager = FileManager.getInstance();
	
	private ConnectionPool pool = ConnectionPool.getInstance();

	private static final CreateParameters CREATE_PARENT_DIRS = new CreateParameters().setDirectory(true).setMakeParents(true);

	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target,
			CopyParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	private URI rename(URI source, URI target, MoveParameters params) throws IOException, SftpException {
		PooledSFTPConnection connection = null;
		try {
			connection = connect(source);
			ChannelSftp channel = connection.getChannelSftp();
			Info sourceInfo = info(source, channel);
			if (sourceInfo == null) {
				throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), source.toString())); //$NON-NLS-1$
			}
			Info targetInfo = info(target, channel);
			boolean targetChanged = false;
			if (((targetInfo != null) && targetInfo.isDirectory()) ||
					(Boolean.TRUE.equals(params.isMakeParents()) && target.toString().endsWith(URIUtils.PATH_SEPARATOR))) {
				target = URIUtils.getChildURI(target, sourceInfo.getName());
				targetChanged = true; // maybe new targetInfo will not be needed 
			}
			if (params.isUpdate() || params.isNoOverwrite()) {
				if (targetChanged) { // obtain new targetInfo if the target has changed
					targetInfo = info(target, channel);
				}
				if (targetInfo != null) {
					if (params.isNoOverwrite()) {
						return target;
					}
					if (params.isUpdate() && (sourceInfo.getLastModified().compareTo(targetInfo.getLastModified()) <= 0)) {
						return target;
					}
				}
			}

			if (Boolean.TRUE.equals(params.isMakeParents())) {
				if (targetChanged) { // obtain new targetInfo if the target has changed
					targetInfo = info(target, channel);
					targetChanged = false;
				}
				if (targetInfo == null) {
					URI parentUri = URIUtils.getParentURI(target);
					create(channel, parentUri, CREATE_PARENT_DIRS);
				}
			}
			if (source.normalize().equals(target.normalize())) {
				throw new SameFileException(source, target);
			}
			channel.rename(getPath(source), getPath(target));
			return target;
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			disconnectQuietly(connection);
		}
	}

	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target,
			MoveParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		URI sourceUri = source.toURI();
		URI targetUri = target.toURI();
		if (sourceUri.getAuthority().equals(targetUri.getAuthority())) {
			try {
				URI result = rename(sourceUri, targetUri, params);
				return result != null ? CloverURI.createSingleURI(result) : null;
			} catch (FileNotFoundException fnfe) {
				throw fnfe;
			} catch (SameFileException sfe) {
				throw sfe;
			} catch (SftpException sftpe) {
				// ignore the exception, try another handler
			} catch (IOException ioe) {
				// ignore the exception, try another handler
			}
		}

		IOperationHandler nextHandler = manager.findNextHandler(Operation.move(source.getScheme(), target.getScheme()), this);
		if (nextHandler != null) {
			return nextHandler.move(source, target, params);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public ReadableContent getInput(SingleCloverURI source,
			ReadParameters params) throws IOException {
		return new SFTPContent(source.toURI());
	}

	@Override
	public WritableContent getOutput(SingleCloverURI target,
			WriteParameters params) throws IOException {
		return new SFTPContent(target.toURI());
	}

	private final ListParameters LIST_NONRECURSIVE = new ListParameters().setRecursive(false);
	
	private void delete(ChannelSftp channel, Info info, DeleteParameters params) throws IOException, SftpException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		URI uri = info.getURI();
		if (info.isDirectory()) {
			if (params.isRecursive()) {
				List<Info> children = list(uri, channel, LIST_NONRECURSIVE);
				for (Info child: children) {
					delete(channel, child, params);
				}
				channel.rmdir(getPath(uri));
			} else {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.cannot_remove_directory"), uri)); //$NON-NLS-1$
			}
		} else {
			channel.rm(getPath(uri));
		}
	}

	private void delete(ChannelSftp channel, URI uri, DeleteParameters params) throws IOException, SftpException {
		Info info = simpleInfo(uri, channel); // CLO-3949
		if (info == null) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), uri.toString())); //$NON-NLS-1$
		}
		delete(channel, info, params);
	}
	
	@Override
	public SingleCloverURI delete(SingleCloverURI target, DeleteParameters params) throws IOException {
		URI uri = target.toURI().normalize();
		PooledSFTPConnection connection = null;
		try {
			connection = connect(uri);
			delete(connection.getChannelSftp(), uri, params);
			return CloverURI.createSingleURI(uri);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			disconnectQuietly(connection);
		}
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI wildcards,
			ResolveParameters params) throws IOException {
		return manager.defaultResolve(wildcards);
	}

	private static class SFTPInfo implements Info {
		
		private final String name;
		private final LsEntry file;
		private final URI uri;
		private final URI parent;
		private final SftpATTRS attrs;
		
		private static final int S_IREAD = 00400; // read by owner
		private static final int S_IWRITE= 00200; // write by owner
		private static final int S_IEXEC = 00100; // execute/search by owner
		
		public SFTPInfo(LsEntry file, String name, URI parent, URI self) throws UnsupportedEncodingException {
			this.file = file;
			this.parent = parent;
			this.name = name;
			this.attrs = file.getAttrs();
			if (file.getAttrs().isDir() && !name.endsWith(URIUtils.PATH_SEPARATOR)) {
				name = name + URIUtils.PATH_SEPARATOR;
			}
			if (self != null) {
				this.uri = self;
			} else {
				this.uri = URIUtils.getChildURI(parent, name);
			}
		}
		
		/**
		 * CLO-3949:
		 * Simplified constructor, file deletion speed optimization.
		 * Does not contain filename,
		 * some methods may throw exceptions upon invocation.
		 * 
		 * @param attrs
		 * @param self
		 */
		private SFTPInfo(SftpATTRS attrs, URI self) {
			this.file = null;
			this.parent = null;
			this.name = null;
			this.uri = self;
			this.attrs = attrs;
		}
		
		@Override
		public String getName() {
			return name;
		}

		@Override
		public URI getURI() {
			return uri;
		}

		@Override
		public URI getParentDir() {
			return parent;
		}

		@Override
		public boolean isDirectory() {
			return attrs.isDir();
		}

		@Override
		public boolean isFile() {
			return !isDirectory() && !isLink();
		}
		
		@Override
		public Boolean isLink() {
			return attrs.isLink();
		}

		@Override
		public Type getType() {
			if (isDirectory()) {
				return Type.DIR;
			} else if (isLink()) {
				return Type.LINK;
			} else {
				return Type.FILE;
			}
		}

		@Override
		public Date getLastModified() {
			return new Date(attrs.getMTime() * 1000L);
		}

		@Override
		public Date getCreated() {
			return null;
		}

		@Override
		public Date getLastAccessed() {
			return new Date(attrs.getATime() * 1000L);
		}

		@Override
		public Long getSize() {
			return attrs.getSize();
		}

		@Override
		public String toString() {
			return uri.toString();
		}

		@Override
		public Boolean isHidden() {
			return file.getFilename().startsWith("."); // FIXME is this correct on Windows? //$NON-NLS-1$
		}

		@Override
		public Boolean canRead() {
			return (attrs.getPermissions() & S_IREAD) != 0;
		}

		@Override
		public Boolean canWrite() {
			return (attrs.getPermissions() & S_IWRITE) != 0;
		}

		@Override
		public Boolean canExecute() {
			return (attrs.getPermissions() & S_IEXEC) != 0;
		}
		
	}

	private Info info(LsEntry file, String name, URI parent, URI self) {
		if (file == null) {
			return null;
		}
		try {
			return new SFTPInfo(file, (name != null) ? name : file.getFilename(), parent, self);
		} catch (IOException ex) {
			return null;
		}
	}
	
	private static String getPath(URI uri) {
		String path = uri.getPath();
		if (StringUtils.isEmpty(path)) {
			path = URIUtils.PATH_SEPARATOR;
		}
		return quote(path);
	}
	
	private static String quote(String path) {
		StringBuilder sb = new StringBuilder();
		int length = path.length();
		for (int i = 0; i < length; i++) {
			char c = path.charAt(i);
			switch (c) {
			case '\\': case '*': case '?':
				sb.append('\\');
			}
			sb.append(c);
		}
		return sb.toString();
	}
	
	/**
	 * CLO-3949:
	 * Speed optimization, only performs STAT for regular files.
	 * 
	 * @param targetUri
	 * @param channel
	 * @return
	 * @throws IOException
	 */
	private Info simpleInfo(URI targetUri, ChannelSftp channel) throws IOException {
		return info(targetUri, channel, true);
	}
	
	/**
	 * 
	 * @param targetUri
	 * @param channel
	 * @param simple - only performs STAT for regular files
	 * @return
	 * @throws IOException
	 * @see <a href="https://bug.javlin.eu/browse/CLO-3949">CLO-3949</a>
	 */
	private Info info(URI targetUri, ChannelSftp channel, boolean simple) throws IOException {
		try {
			String path = getPath(targetUri);
			SftpATTRS attrs = channel.stat(path);
			if (!attrs.isDir()) {
				if (simple) {
					// speed optimization, does not contain filename!
					return new SFTPInfo(attrs, targetUri);
				} else {
					@SuppressWarnings("unchecked")
					Vector<LsEntry> files = channel.ls(path);
					if ((files != null) && !files.isEmpty()) {
						return info(files.get(0), null, null, targetUri);
					}
				}
			} else {
				@SuppressWarnings("unchecked")
				Vector<LsEntry> files = channel.ls(path);
				if ((files != null) && !files.isEmpty() && (files.get(0) != null)) {
					for (LsEntry file: files) {
						if ((file != null) && file.getAttrs().isDir() && file.getFilename().equals(URIUtils.CURRENT_DIR_NAME)) {
							URI parentUri = URIUtils.getParentURI(targetUri);
							if (parentUri != null) {
								String fileName = parentUri.relativize(targetUri).toString();
								fileName = URIUtils.urlDecode(fileName);
								return info(file, fileName, null, targetUri);
							}
							return info(file, null, null, targetUri);
						}
						
					}
					return info(files.get(0), null, null, targetUri);
				}
			}
		} catch (SftpException sftpe) {
			if (sftpe.id != ChannelSftp.SSH_FX_NO_SUCH_FILE) { // other than No such file
				throw new IOException("Failed to get SFTP file info", sftpe);
			}
		} catch (Exception e) {
			throw new IOException("Failed to get SFTP file info", e);
		}
		return null;
	}

	private Info info(URI targetUri, ChannelSftp channel) throws IOException {
		return info(targetUri, channel, false);
	}
	
	private PooledSFTPConnection connect(URI uri) throws IOException {
		Authority key = new SFTPAuthority(uri, null);
		try {
			return (PooledSFTPConnection) pool.borrowObject(key);
		} catch (IOException ioe) {
			throw ioe;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	private void disconnectQuietly(PooledSFTPConnection connection) {
		// make sure the object is returned to the pool
		if (connection != null) {
			try {
				pool.returnObject(connection.getAuthority(), connection);
			} catch (Exception ex) {
				log.debug("Failed to return SFTP connection to the pool");
			}
		}
	}
	
	private List<Info> list(URI parentUri, ChannelSftp channel, ListParameters params) throws IOException, SftpException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		Info rootInfo = info(parentUri, channel);
		if (rootInfo == null) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), parentUri.toString())); //$NON-NLS-1$
		}
		if (parentUri.toString().endsWith(URIUtils.PATH_SEPARATOR) && !rootInfo.isDirectory()) {
			throw new FileNotFoundException(format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), parentUri)); //$NON-NLS-1$
		}
		if (!rootInfo.isDirectory()) {
			return Arrays.asList(rootInfo);
		} else {
			@SuppressWarnings("unchecked")
			Vector<LsEntry> files = channel.ls(getPath(parentUri));
			List<Info> result = new ArrayList<Info>(files.size());
			for (LsEntry file: files) {
				if ((file != null) && !file.getFilename().equals(URIUtils.CURRENT_DIR_NAME) && !file.getFilename().equals(URIUtils.PARENT_DIR_NAME)) {
					Info child = info(file, null, parentUri, null);
					result.add(child);
					if (params.isRecursive() && file.getAttrs().isDir()) {
						result.addAll(list(child.getURI(), channel, params));
					}
				}
			}
			return result;
		}
	}

	@Override
	public List<Info> list(SingleCloverURI parent, ListParameters params)
			throws IOException {
		URI uri = parent.toURI();
		PooledSFTPConnection connection = null;
		try {
			connection = connect(uri);
			return list(uri, connection.getChannelSftp(), params);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			disconnectQuietly(connection);
		}
	}
	
	@Override
	public Info info(SingleCloverURI target, InfoParameters params) throws IOException {
		return info(target.toURI());
	}

	private Info info(URI uri) throws IOException {
		PooledSFTPConnection connection = null;
		try {
			connection = connect(uri);
			return info(uri, connection.getChannelSftp());
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			disconnectQuietly(connection);
		}
	}

	private void createFile(ChannelSftp ftp, String path) throws SftpException {
		ftp.put(new ByteArrayInputStream(new byte[0]), path);
	}

	private void setLastModified(ChannelSftp channel, String path, long millis) throws SftpException {
		long secs = millis / 1000;
		channel.setMtime(path, (int) secs);
	}

	private void create(ChannelSftp channel, URI uri, CreateParameters params) throws IOException, SftpException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		boolean createDirectory = Boolean.TRUE.equals(params.isDirectory());
		boolean createParents = Boolean.TRUE.equals(params.isMakeParents());
		Info fileInfo = info(uri, channel);
		String path = getPath(uri);
		Date lastModified = params.getLastModified();
		if (fileInfo == null) { // does not exist
			if (createParents) {
				URI parentUri = URIUtils.getParentURI(uri);
				create(channel, parentUri, params.clone().setDirectory(true));
			}
			if (createDirectory) {
				channel.mkdir(path);
			} else {
				createFile(channel, path);
			}
			if (lastModified != null) {
				setLastModified(channel, path, lastModified.getTime());
			}
		} else {
			if (createDirectory != fileInfo.isDirectory()) {
				throw new IOException(MessageFormat.format(createDirectory ? FileOperationMessages.getString("IOperationHandler.exists_not_directory") : FileOperationMessages.getString("IOperationHandler.exists_not_file"), uri)); //$NON-NLS-1$ //$NON-NLS-2$
			}
			setLastModified(channel, path, lastModified != null ? lastModified.getTime() : System.currentTimeMillis());
		}
	}

	@Override
	public SingleCloverURI create(SingleCloverURI target, CreateParameters params) throws IOException {
		URI uri = target.toURI().normalize();
		PooledSFTPConnection connection = null;
		try {
			connection = connect(uri);
			create(connection.getChannelSftp(), uri, params);
			return CloverURI.createSingleURI(uri);
		} catch (Exception e) {
			throw new IOException(e);
		} finally {
			disconnectQuietly(connection);
		}
	}

	@Override
	public File getFile(SingleCloverURI uri, FileParameters params)
			throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getPriority(Operation operation) {
		return TOP_PRIORITY;
	}

	@Override
	public boolean canPerform(Operation operation) {
		switch (operation.kind) {
		case READ:
		case WRITE:
		case LIST:
		case INFO:
		case RESOLVE:
		case DELETE:
		case CREATE:
			return operation.scheme().equalsIgnoreCase(SFTP_SCHEME);
//		case COPY: // remote copying is not supported by SFTP
		case MOVE: // can be achieved by renaming, but only within a single host 
			return operation.scheme(0).equalsIgnoreCase(SFTP_SCHEME) 
					&& operation.scheme(1).equalsIgnoreCase(SFTP_SCHEME);
		}
		return false;
	}

	@Override
	public String toString() {
		return "PooledSFTPOperationHandler"; //$NON-NLS-1$
	}
	
	private class SFTPContent implements Content {
		
		private final URI uri;

		public SFTPContent(URI uri) {
			this.uri = uri;
		}

		@Override
		public ReadableByteChannel read() throws IOException {
			PooledSFTPConnection obj = connect(uri);
			return Channels.newChannel(obj.getInputStream(getPath(uri)));
		}

		@Override
		public WritableByteChannel write() throws IOException {
			PooledSFTPConnection obj = connect(uri);
			return Channels.newChannel(obj.getOutputStream(getPath(uri), ChannelSftp.OVERWRITE));
		}

		@Override
		public WritableByteChannel append() throws IOException {
			PooledSFTPConnection obj = connect(uri);
			return Channels.newChannel(obj.getOutputStream(getPath(uri), ChannelSftp.APPEND));
		}
		
	}
}
