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
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
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

public class FTPOperationHandler implements IOperationHandler {
	
	public static final String FTP_SCHEME = "ftp"; //$NON-NLS-1$
	
	private FileManager manager = FileManager.getInstance();
	
	@Override
	public int getPriority(Operation operation) {
		return Integer.MAX_VALUE;
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
	
	private class FTPInfo implements Info {
		
		private final FTPFile file;
		private final URI uri;
		private final URI parent;
		
		public FTPInfo(FTPFile file, URI parent) throws UnsupportedEncodingException {
			this.file = file;
			this.parent = parent;
			String name = file.getName();
			String encodedName = URLEncoder.encode(name, "US-ASCII"); //$NON-NLS-1$
			if (file.isDirectory() && !name.endsWith("/")) { //$NON-NLS-1$
				encodedName = encodedName + "/"; //$NON-NLS-1$
			}
			URI tmp = URIUtils.getChildURI(parent, encodedName);
			this.uri = tmp;
		}

		@Override
		public String getName() {
			return file.getName();
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
			return file.getTimestamp().getTime();
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

	private String[] getUserInfo(URI uri) {
		String userInfo = uri.getUserInfo();
		if (userInfo == null) return new String[] {""}; //$NON-NLS-1$
		return decodeString(userInfo).split(":"); //$NON-NLS-1$
	}

	private FTPClient connect(URI uri) throws IOException {
		FTPClient ftp = new FTPClient();
		ftp.setListHiddenFiles(true);
		String[] user = getUserInfo(uri);
		try {
			int port = uri.getPort();
			if (port < 0) {
				port = 21;
			}
			ftp.connect(uri.getHost(), port);
			if(!ftp.login(user.length >= 1 ? user[0] : "", user.length >= 2 ? user[1] : "")) { //$NON-NLS-1$ //$NON-NLS-2$
	            ftp.logout();
	            throw new IOException(FileOperationMessages.getString("FTPOperationHandler.authentication_failed")); //$NON-NLS-1$
	        }
			ftp.enterLocalPassiveMode();
			
			int reply = ftp.getReplyCode();
	        if (!FTPReply.isPositiveCompletion(reply)) {
	        	throw new IOException(FileOperationMessages.getString("FTPOperationHandler.connection_failed")); //$NON-NLS-1$
	        }
	        return ftp;
		} catch (IOException ioe) {
			if (ftp != null && ftp.isConnected()) {
				try {
					ftp.disconnect();
				} catch (IOException e) {
					e.printStackTrace(); // FIXME log the error
				}
			}
			throw new IOException(FileOperationMessages.getString("FTPOperationHandler.connection_failed"), ioe); //$NON-NLS-1$
		}
	}
	
	private Info info(FTPFile file, URI parent) {
		if (file == null) {
			return null;
		}
		try {
			return new FTPInfo(file, parent);
		} catch (IOException ex) {
			return null;
		}
	}
	
	private Info info(URI targetUri, FTPClient ftp) {
		try {
//			System.err.println(connection.cd(targetUri.getPath()) ? "DIR" : "FILE");
			URI parentUri = URIUtils.getParentURI(targetUri);
			String fileName = parentUri.relativize(targetUri).toString();
			if (fileName.endsWith(URIUtils.PATH_SEPARATOR)) {
				fileName = fileName.substring(0, fileName.length()-1);
			}
			FTPFile[] files = ftp.listFiles(parentUri.getPath());
			if (files != null) {
				for (FTPFile file: files) {
					if ((file != null) && !file.getName().equals(URIUtils.CURRENT_DIR_NAME) && !file.getName().equals(URIUtils.PARENT_DIR_NAME)) {
						Info info = info(file, parentUri);
						if (info.getName().equals(fileName)) {
							return info;
						}
					}
				}
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return null;
	}
	
	private void disconnect(FTPClient ftp) {
		if ((ftp != null) && ftp.isConnected()) {
			try {
				ftp.disconnect();
			} catch(IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	private Info info(URI targetUri) throws IOException {
		FTPClient ftp = null;
		try {
			ftp = connect(targetUri);
			return info(targetUri, ftp);
		} finally {
			disconnect(ftp);
		}
	}

	@Override
	public Info info(SingleCloverURI target, InfoParameters params) throws IOException {
		return info(target.toURI());
	}
	
	private boolean createFile(FTPClient ftp, String path) throws IOException {
		return ftp.storeFile(path, new ByteArrayInputStream(new byte[0]));
	}
	
	private boolean create(FTPClient ftp, URI uri, CreateParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		boolean success = true;
		boolean createDirectory = Boolean.TRUE.equals(params.isDirectory());
		boolean createParents = Boolean.TRUE.equals(params.isMakeParents());
		Info fileInfo = info(uri);
		if (fileInfo == null) { // does not exist
			if (createParents) {
				URI parentUri = URIUtils.getParentURI(uri);
				create(ftp, parentUri, params.clone().setDirectory(true));
			}
			String path = uri.getPath();
			if (createDirectory) {
				success = ftp.makeDirectory(path);
			} else {
				success = createFile(ftp, path);
			}
		}
		return success;
	}
	
	

	@Override
	public boolean create(SingleCloverURI target, CreateParameters params) throws IOException {
		if (params.getLastModified() != null) {
			throw new UnsupportedOperationException(FileOperationMessages.getString("FTPOperationHandler.setting_date_not_supported")); //$NON-NLS-1$
		}
		URI uri = target.toURI();
		FTPClient ftp = null;
		try {
			ftp = connect(uri);
			return create(ftp, uri, params);
		} finally {
			disconnect(ftp);
		}
		
	}

	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target, CopyParameters params) {
		throw new UnsupportedOperationException();
	}
	
	private URI rename(URI source, URI target, MoveParameters params) throws IOException {
		FTPClient ftp = null;
		try {
			ftp = connect(source);
			Info sourceInfo = info(source, ftp);
			if (sourceInfo == null) {
				throw new FileNotFoundException(source.toString());
			}
			Info targetInfo = info(target, ftp);
			boolean targetChanged = false;
			if (targetInfo != null && targetInfo.isDirectory()) {
				target = URIUtils.getChildURI(target, sourceInfo.getName());
				targetChanged = true; // maybe new targetInfo will not be needed 
			}
			if (params.isUpdate() || params.isNoOverwrite()) {
				if (targetChanged) { // obtain new targetInfo if the target has changed
					targetInfo = info(target, ftp);
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
			if (source.normalize().equals(target.normalize())) {
				throw new SameFileException(source, target);
			}
			return ftp.rename(source.getPath(), target.getPath()) ? target : null;
		} finally {
			disconnect(ftp);
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
				return result != null ? CloverURI.createSingleURI(result) : null;
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
			}
			return ftp.removeDirectory(uri.getPath());
		} else {
			return ftp.deleteFile(uri.getPath());
		}
	}

	private boolean delete(FTPClient ftp, URI uri, DeleteParameters params) throws IOException {
		Info info = info(uri, ftp);
		if (info == null) {
			throw new FileNotFoundException(uri.toString());
		}
		return delete(ftp, info, params);
	}
	
	@Override
	public boolean delete(SingleCloverURI target, DeleteParameters params) throws IOException {
		URI uri = target.toURI();
		FTPClient ftp = null;
		try {
			ftp = connect(uri);
			return delete(ftp, uri, params);
		} finally {
			disconnect(ftp);
		}
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI wildcards, ResolveParameters params) {
		return manager.defaultResolve(wildcards);
	}
	
	private List<Info> list(URI parentUri, FTPClient ftp, ListParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		Info rootInfo = info(parentUri, ftp);
		if (rootInfo == null) {
			throw new FileNotFoundException(parentUri.toString());
		}
		if (parentUri.toString().endsWith(URIUtils.PATH_SEPARATOR) && !rootInfo.isDirectory()) {
			throw new FileNotFoundException(format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), parentUri)); //$NON-NLS-1$
		}
		if (!rootInfo.isDirectory()) {
			return Arrays.asList(rootInfo);
		} else {
			FTPFile[] files = ftp.listFiles(parentUri.getPath());
			List<Info> result = new ArrayList<Info>(files.length);
			for (FTPFile file: files) {
				if ((file != null) && !file.getName().equals(URIUtils.CURRENT_DIR_NAME) && !file.getName().equals(URIUtils.PARENT_DIR_NAME)) {
					Info child = info(file, parentUri);
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
		FTPClient ftp = connect(parentUri);
		try {
			ftp = connect(parentUri);
			return list(parentUri, ftp, params);
		} finally {
			disconnect(ftp);
		}
	}
	
	@Override
	public ReadableContent getInput(SingleCloverURI source, ReadParameters params) {
		return new URLContent(source.toURI());
	}

	@Override
	public WritableContent getOutput(SingleCloverURI target, WriteParameters params) {
		return new URLContent(target.toURI());
	}

	@Override
	public File getFile(SingleCloverURI uri, FileParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return "FTPOperationHandler"; //$NON-NLS-1$
	}
	
	

}
