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
import java.net.URLDecoder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPCmd;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.parser.MLSxEntryParser;
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
import org.jetel.component.fileoperation.URLOperationHandler.URLContent;
import org.jetel.component.fileoperation.pool.Authority;
import org.jetel.component.fileoperation.pool.ConnectionPool;
import org.jetel.component.fileoperation.pool.DefaultAuthority;
import org.jetel.component.fileoperation.pool.PooledFTPConnection;
import org.jetel.component.fileoperation.pool.PooledFTPConnection.WriteMode;
import org.jetel.util.string.StringUtils;
import org.jetel.util.string.UnicodeBlanks;

public class PooledFTPOperationHandler implements IOperationHandler {
	
	public static final String FTP_SCHEME = "ftp"; //$NON-NLS-1$
	
	private FileManager manager = FileManager.getInstance();
	
	private ConnectionPool pool = ConnectionPool.getInstance();
	
	private static final Log log = LogFactory.getLog(PooledFTPOperationHandler.class);
	
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
			return operation.scheme().equalsIgnoreCase(FTP_SCHEME);
//		case COPY: // remote copying is not supported by FTP
		case MOVE: // can be achieved by renaming, but only within a single host 
			return operation.scheme(0).equalsIgnoreCase(FTP_SCHEME)
					&& operation.scheme(1).equalsIgnoreCase(FTP_SCHEME);
		}
		return false;
	}
	
	// matches the FILENAME part of a path: /dir/subdir/subsubdir/FILENAME
	private static final Pattern FILENAME_PATTERN = Pattern.compile(".*/([^/]+/?)"); //$NON-NLS-1$
	
	private static class FTPInfo implements Info {
		
		private final FTPFile file;
		private final URI uri;
		private final URI parent;
		private final String name;
		
		public FTPInfo(FTPFile file, URI parent, URI self) throws UnsupportedEncodingException {
			this.file = file;
			this.parent = parent;
			String name = file.getName();
			Matcher m = FILENAME_PATTERN.matcher(name);
			if (m.matches()) {
				name = m.group(1); // some FTPs return full file paths as names, we want only the filename 
			}
			if (name.equals("/")) {
				name = ""; // root directory has no name
			} else if (name.endsWith(URIUtils.PATH_SEPARATOR)) {
				name = name.substring(0, name.length()-1);
			}
			this.name = name;
			// name is modified just for the URI
			if (file.isDirectory() && !name.endsWith(URIUtils.PATH_SEPARATOR)) {
				name = name + URIUtils.PATH_SEPARATOR;
			}
			if (self != null) {
				this.uri = self;
			} else {
				this.uri = URIUtils.getChildURI(parent, name);
			}
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
			return file.isDirectory();
		}

		@Override
		public boolean isFile() {
			return file.isFile();
		}
		
		@Override
		public Boolean isLink() {
			return file.isSymbolicLink();
		}

		@Override
		public Type getType() {
			if (isDirectory()) {
				return Type.DIR;
			} else if (isFile()) {
				return Type.FILE;
			} else if (isLink()) {
				return Type.LINK;
			}
			return Type.OTHER;
		}

		@Override
		public Date getLastModified() {
			return file.getTimestamp() != null? file.getTimestamp().getTime(): null;
		}

		@Override
		public Date getCreated() {
			return null;
		}

		@Override
		public Date getLastAccessed() {
			return null;
		}

		@Override
		public Long getSize() {
			return file.getSize();
		}

		@Override
		public String toString() {
			return uri.toString();
		}

		@Override
		public Boolean isHidden() {
			return file.getName().startsWith("."); // FIXME is this correct on Windows? //$NON-NLS-1$
		}

		@Override
		public Boolean canRead() {
			return file.hasPermission(FTPFile.USER_ACCESS, FTPFile.READ_PERMISSION);
		}

		@Override
		public Boolean canWrite() {
			return file.hasPermission(FTPFile.USER_ACCESS, FTPFile.WRITE_PERMISSION);
		}

		@Override
		public Boolean canExecute() {
			return file.hasPermission(FTPFile.USER_ACCESS, FTPFile.EXECUTE_PERMISSION);
		}
		
	}
	
	private static final String ENCODING = "US-ASCII"; //$NON-NLS-1$
	
	private String decodeString(String s) {
		try {
			return URLDecoder.decode(s, ENCODING);
		} catch (UnsupportedEncodingException e) {
			return s;
		}
	}

	protected String[] getUserInfo(URI uri) {
		String userInfo = uri.getUserInfo();
		if (userInfo == null) return new String[] {""}; //$NON-NLS-1$
		return decodeString(userInfo).split(":"); //$NON-NLS-1$
	}

	protected PooledFTPConnection connect(URI uri) throws IOException {
		try {
			Authority authority = new DefaultAuthority(uri);
			return (PooledFTPConnection) pool.borrowObject(authority);
		} catch (IOException ioe) {
			throw ioe;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	private Info info(FTPFile file, URI parent, URI self) {
		if (file == null) {
			return null;
		}
		try {
			return new FTPInfo(file, parent, self);
		} catch (IOException ex) {
			return null;
		}
	}
	
	private static String quote(String path) {
		StringBuilder sb = new StringBuilder();
		int length = path.length();
		for (int i = 0; i < length; i++) {
			char c = path.charAt(i);
			switch (c) {
			case '*': 
				sb.append("%2A"); //$NON-NLS-1$
				break;
			case '?':
				sb.append("%3F"); //$NON-NLS-1$
				break;
			default: sb.append(c);
			}
		}
		return sb.toString();
	}

	private String getPath(URI uri) {
		String result = quote(uri.getPath()); 
		return StringUtils.isEmpty(result) ? URIUtils.PATH_SEPARATOR : result;
	}
	
	/**
	 * CLO-4118:
	 * Performs the MLST command.
	 * Copied from {@link FTPClient#mlistFile(String)}.
	 * 
	 * GlobalScape EFT does not return the leading space,
	 * as required by <a href="http://tools.ietf.org/html/rfc3659#section-7.2">RFC 3659</a>.
	 * This causes the entry to be stripped of the first character 
	 * - the "Type" fact is turned to "ype". 
	 * 
	 * @param ftp		FTP client
	 * @param pathname	remote path
	 * @return FTPFile or <code>null</code>	
	 * @throws IOException
	 * 
	 * @see {@link FTPClient#mlistFile(String)}
	 * @see <a href="http://tools.ietf.org/html/rfc3659#section-7.2">RFC 3659 - Extensions to FTP</a>
	 */
	private FTPFile mlistFile(FTPClient ftp, String pathname) throws IOException {
        boolean success = FTPReply.isPositiveCompletion(ftp.sendCommand(FTPCmd.MLST, pathname));
        if (success) {
            String entry = ftp.getReplyStrings()[1];
            char firstChar = entry.charAt(0);
            if (UnicodeBlanks.isBlank(firstChar)) {
            	// skip leading space for parser
            	entry = entry.substring(1);
            }
            return MLSxEntryParser.parseEntry(entry);
        } else {
            return null;
        }
//		return ftp.mlistFile(pathname);
	}
	
	/**
	 * Implementation using the MLST command, if available. 
	 * 
	 * @param targetUri
	 * @param ftp
	 * @return
	 */
	private Info info(URI targetUri, FTPClient ftp) {
		try {
			if (ftp.hasFeature(FTPCmd.MLST.getCommand())) {
				// Pure-FTPd does not understand .. and . in the path, hence we use URI.normalize()
				String path = getPath(targetUri.normalize());
				if (path.equals(URIUtils.PATH_SEPARATOR)) {
					// CLO-4118: root directory, GlobalScape EFT disallows MLST
					FTPFile root = new FTPFile();
					root.setType(FTPFile.DIRECTORY_TYPE);
					root.setName("");
					return info(root, null, targetUri);
				}
				FTPFile file = mlistFile(ftp, path);
				if (file != null) {
					return new FTPInfo(file, null, targetUri);
				} else {
					return null;
				}
			}
		} catch (IOException ioe) {
			log.debug(MessageFormat.format("File metadata reading failed: {0}", targetUri), ioe);
		}
		
		return infoFallback(targetUri, ftp);
	}
	
	/**
	 * If available, lists directory contents using the MLSD command.
	 * Otherwise uses LIST command.
	 * 
	 * @param path
	 * @param ftp
	 * @return
	 * @throws IOException
	 */
	private FTPFile[] listFiles(String path, FTPClient ftp) throws IOException {
		if (ftp.hasFeature(FTPCmd.MLSD.getCommand())) {
			return ftp.mlistDir(path);
		} else {
			return ftp.listFiles(path);
		}
	}
	
	/**
	 * Legacy fallback implementation using the LIST command.
	 * 
	 * @param targetUri
	 * @param ftp
	 * @return
	 */
	private Info infoFallback(URI targetUri, FTPClient ftp) {
		if (getPath(targetUri.normalize()).equals(URIUtils.PATH_SEPARATOR)) {
			FTPFile root = new FTPFile();
			root.setType(FTPFile.DIRECTORY_TYPE);
			root.setName("");
			return info(root, null, targetUri);
		} else {
			String path = getPath(targetUri);
			try {
				if (ftp.changeWorkingDirectory(path)) { // a directory
					URI parentUri = URIUtils.getParentURI(targetUri);
					if (parentUri != null) {
						FTPFile[] files = ftp.listFiles(getPath(parentUri));
						if (files.length == 0) {
							throw new IOException(MessageFormat.format(FileOperationMessages.getString("FTPOperationHandler.parent_dir_listing_failed"), targetUri)); //$NON-NLS-1$
						}
						String fileName = parentUri.relativize(targetUri).toString();
						if (fileName.endsWith(URIUtils.PATH_SEPARATOR)) {
							fileName = fileName.substring(0, fileName.length()-1);
						}
						fileName = URIUtils.urlDecode(fileName);
						for (FTPFile file: files) {
							if ((file != null) && file.isDirectory() && file.getName().equals(fileName)) {
								return info(file, null, targetUri);
							}
						}
					}
				} else { // a file
					FTPFile[] files = ftp.listFiles(path);
					if ((files != null) && (files.length != 0) && (files[0] != null)) {
						return info(files[0], null, targetUri);
					}
				}
			} catch (IOException ioe) {
				log.warn(MessageFormat.format("File metadata reading failed: {0}", targetUri), ioe);
			}
			
		}
		return null;
	}

	private void disconnect(PooledFTPConnection connection) {
		if (connection != null) {
			try {
				pool.returnObject(connection.getAuthority(), connection);
			} catch (Exception ex) {
				log.debug("Failed to return FTP connection to the pool", ex);
			}
		}
	}

	private Info info(URI targetUri) throws IOException {
		PooledFTPConnection ftp = null;
		try {
			ftp = connect(targetUri);
			return info(targetUri, ftp.getFtpClient());
		} finally {
			disconnect(ftp);
		}
	}

	@Override
	public Info info(SingleCloverURI target, InfoParameters params) throws IOException {
		return info(target.toURI());
	}
	
	private boolean createFile(FTPClient ftp, String path) throws IOException {
		log.debug(MessageFormat.format("Creating {0}", path));
		ftp.storeFile(path, new ByteArrayInputStream(new byte[0]));
		int replyCode = ftp.getReplyCode();
		boolean result = FTPReply.isPositiveCompletion(replyCode) || FTPReply.isPositiveIntermediate(replyCode) || FTPReply.isPositivePreliminary(replyCode);
		if (!result) {
			result = FTPReply.isPositiveCompletion(replyCode);
			log.debug(MessageFormat.format("Failed to create {0}: {1}", path, ftp.getReplyString()));
		}
		return result;
	}
	
	private boolean create(FTPClient ftp, URI uri, CreateParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		boolean success = true;
		boolean createDirectory = Boolean.TRUE.equals(params.isDirectory());
		boolean createParents = Boolean.TRUE.equals(params.isMakeParents());
		Info fileInfo = info(uri, ftp);
		if (fileInfo == null) { // does not exist
			URI parentUri = URIUtils.getParentURI(uri);
			if (createParents) {
				create(ftp, parentUri, params.clone().setDirectory(true));
			} else {
				Info parentInfo = info(parentUri, ftp);
				if (parentInfo == null) {
					throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), uri.toString())); //$NON-NLS-1$
				}
			}
			String path = getPath(uri);
			if (createDirectory) {
				success = ftp.makeDirectory(path);
			} else {
				success = createFile(ftp, path);
			}
		} else {
			Boolean directory = params.isDirectory();
			if ((directory != null) && !directory.equals(fileInfo.isDirectory())) {
				throw new IOException(MessageFormat.format(createDirectory ? FileOperationMessages.getString("IOperationHandler.exists_not_directory") : FileOperationMessages.getString("IOperationHandler.exists_not_file"), fileInfo.getURI())); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
		return success;
	}
	
	

	@Override
	public SingleCloverURI create(SingleCloverURI target, CreateParameters params) throws IOException {
		if (params.getLastModified() != null) {
			throw new UnsupportedOperationException(FileOperationMessages.getString("FTPOperationHandler.setting_date_not_supported")); //$NON-NLS-1$
		}
		URI uri = target.toURI().normalize();
		PooledFTPConnection ftp = null;
		try {
			ftp = connect(uri);
			if (create(ftp.getFtpClient(), uri, params)) {
				return CloverURI.createSingleURI(uri);
			}
		} finally {
			disconnect(ftp);
		}
		
		return null;
	}

	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target, CopyParameters params) {
		throw new UnsupportedOperationException();
	}
	
	private static final CreateParameters CREATE_PARENT_DIRS = new CreateParameters().setDirectory(true).setMakeParents(true);
	
	private URI rename(URI source, URI target, MoveParameters params) throws IOException {
		PooledFTPConnection connection = null;
		try {
			connection = connect(source);
			FTPClient ftp = connection.getFtpClient();
			Info sourceInfo = info(source, ftp);
			if (sourceInfo == null) {
				throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), source.toString())); //$NON-NLS-1$
			} else if (!sourceInfo.isDirectory() && target.getPath().endsWith(URIUtils.PATH_SEPARATOR)) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), source)); //$NON-NLS-1$
			}
			Info targetInfo = info(target, ftp);
			boolean targetChanged = false;
			if (((targetInfo != null) && targetInfo.isDirectory()) ||
					(Boolean.TRUE.equals(params.isMakeParents()) && target.toString().endsWith(URIUtils.PATH_SEPARATOR))) {
				target = URIUtils.getChildURI(target, sourceInfo.getName());
				targetChanged = true; // maybe new targetInfo will not be needed 
			}
			if (params.isUpdate() || params.isNoOverwrite()) {
				if (targetChanged) { // obtain new targetInfo if the target has changed
					targetInfo = info(target, ftp);
					targetChanged = false;
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
					targetInfo = info(target, ftp);
					targetChanged = false;
				}
				if (targetInfo == null) {
					URI parentUri = URIUtils.getParentURI(target);
					create(ftp, parentUri, CREATE_PARENT_DIRS);
				}
			}
			if (source.normalize().equals(target.normalize())) {
				throw new SameFileException(source, target);
			}
			return ftp.rename(getPath(source), getPath(target)) ? target : null;
		} finally {
			disconnect(connection);
		}
	}
	
	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		URI sourceUri = source.toURI();
		URI targetUri = target.toURI();
		if (sourceUri.getAuthority().equals(targetUri.getAuthority())) {
			try {
				URI result = rename(sourceUri, targetUri, params);
				if (result != null) {
					return CloverURI.createSingleURI(result);
				}
			} catch (FileNotFoundException fnfe) {
				throw fnfe;
			} catch (SameFileException sfe) {
				throw sfe;
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
	
	private final ListParameters LIST_NONRECURSIVE = new ListParameters().setRecursive(false);
	
	private boolean delete(FTPClient ftp, Info info, DeleteParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		URI uri = info.getURI();
		if (info.isDirectory()) {
			if (params.isRecursive()) {
				List<Info> children = list(uri, ftp, LIST_NONRECURSIVE);
				for (Info child: children) {
					delete(ftp, child, params);
				}
				return ftp.removeDirectory(getPath(uri));
			} else {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.cannot_remove_directory"), uri)); //$NON-NLS-1$
			}
		} else {
			return ftp.deleteFile(getPath(uri));
		}
	}

	private boolean delete(FTPClient ftp, URI uri, DeleteParameters params) throws IOException {
		Info info = info(uri, ftp);
		if (info == null) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), uri.toString())); //$NON-NLS-1$
		}
		if (!info.isDirectory() && uri.toString().endsWith(URIUtils.PATH_SEPARATOR)) {
			throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), uri)); //$NON-NLS-1$
		}
		return delete(ftp, info, params);
	}
	
	@Override
	public SingleCloverURI delete(SingleCloverURI target, DeleteParameters params) throws IOException {
		URI uri = target.toURI().normalize();
		PooledFTPConnection ftp = null;
		try {
			ftp = connect(uri);
			if (delete(ftp.getFtpClient(), uri, params)) {
				return CloverURI.createSingleURI(uri);
			}
		} finally {
			disconnect(ftp);
		}
		return null;
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI wildcards, ResolveParameters params) throws IOException {
		return manager.defaultResolve(wildcards);
	}
	
	private List<Info> list(URI parentUri, FTPClient ftp, ListParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		Info rootInfo = info(parentUri, ftp);
		if (rootInfo == null) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), parentUri.toString())); //$NON-NLS-1$
		}
		if (parentUri.toString().endsWith(URIUtils.PATH_SEPARATOR) && !rootInfo.isDirectory()) {
			throw new FileNotFoundException(format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), parentUri)); //$NON-NLS-1$
		}
		if (!rootInfo.isDirectory()) {
			return Arrays.asList(rootInfo);
		} else {
			FTPFile[] files = listFiles(getPath(parentUri), ftp);
			List<Info> result = new ArrayList<Info>(files.length);
			for (FTPFile file: files) {
				if ((file != null) && !file.getName().equals(URIUtils.CURRENT_DIR_NAME) && !file.getName().equals(URIUtils.PARENT_DIR_NAME)) {
					Info child = info(file, parentUri, null);
					result.add(child);
					if (params.isRecursive() && file.isDirectory()) {
						result.addAll(list(child.getURI(), ftp, params));
					}
				}
			}
			return result;
		}
	}

	@Override
	public List<Info> list(SingleCloverURI parent, ListParameters params) throws IOException {
		URI parentUri = parent.toURI();
		PooledFTPConnection ftp = null;
		try {
			ftp = connect(parentUri);
			return list(parentUri, ftp.getFtpClient(), params);
		} finally {
			disconnect(ftp);
		}
	}
	
	@Override
	public ReadableContent getInput(SingleCloverURI source, ReadParameters params) {
		return new URLContent(source.toURI());
	}

	/**
	 * Used in FTPSOperationHandler.
	 * Seems to handle connections inefficiently,
	 * which may cause the streams to unexpectedly return
	 * zero bytes. Until fixed, use {@link URLContent}
	 * where possible.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Aug 29, 2012
	 */
	protected class FTPContent implements Content {
		
		private final URI uri;
		
		public FTPContent(URI uri) {
			this.uri = uri;
		}

		@Override
		public ReadableByteChannel read() throws IOException {
			PooledFTPConnection connection = connect(uri);
			return Channels.newChannel(connection.getInputStream(getPath(uri)));
		}

		@Override
		public WritableByteChannel write() throws IOException {
			PooledFTPConnection connection = connect(uri);
			Info info = info(uri, connection.getFtpClient());
			if ((info != null) && info.isDirectory()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.exists_not_file"), uri)); //$NON-NLS-1$
			}
			return Channels.newChannel(connection.getOutputStream(getPath(uri), PooledFTPConnection.WriteMode.OVERWRITE));
		}

		@Override
		public WritableByteChannel append() throws IOException {
			PooledFTPConnection connection = connect(uri);
			Info info = info(uri, connection.getFtpClient());
			if ((info != null) && info.isDirectory()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.exists_not_file"), uri)); //$NON-NLS-1$
			}
			return Channels.newChannel(connection.getOutputStream(getPath(uri), WriteMode.APPEND));
		}
		
	}

	@Override
	public WritableContent getOutput(SingleCloverURI target, WriteParameters params) {
		return new URLContent(target.toURI()) {
			
			@Override
			public WritableByteChannel write() throws IOException {
				PooledFTPConnection connection = null;
				try {
					connection = connect(uri);
					Info info = info(uri, connection.getFtpClient());
					if ((info != null) && info.isDirectory()) {
						throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.exists_not_file"), uri)); //$NON-NLS-1$
					}
				} finally {
					disconnect(connection);
				}
				return super.write();
			}

			@Override
			public WritableByteChannel append() throws IOException {
				PooledFTPConnection connection = connect(uri);
				Info info = info(uri, connection.getFtpClient());
				if ((info != null) && info.isDirectory()) {
					throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.exists_not_file"), uri)); //$NON-NLS-1$
				}
				return Channels.newChannel(connection.getOutputStream(getPath(uri), WriteMode.APPEND));
			}
			
		};
	}

	@Override
	public File getFile(SingleCloverURI uri, FileParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return "PooledFTPOperationHandler"; //$NON-NLS-1$
	}
	
	

}
