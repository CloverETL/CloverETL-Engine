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
import java.util.Vector;

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
import org.jetel.util.protocols.sftp.SFTPConnection.URLUserInfoIteractive;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UserInfo;

public class SFTPOperationHandler implements IOperationHandler {

	public static final String SFTP_SCHEME = "sftp"; //$NON-NLS-1$
	
	private final JSch jsch = new JSch();

	private FileManager manager = FileManager.getInstance();
	
	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target,
			CopyParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	private URI rename(URI source, URI target, MoveParameters params) throws IOException, SftpException {
		ChannelSftp ftp = null;
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
			ftp.rename(quote(source.getPath()), quote(target.getPath()));
			return target;
		} finally {
			disconnect(ftp);
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
	
	private void delete(ChannelSftp ftp, Info info, DeleteParameters params) throws IOException, SftpException {
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
			ftp.rmdir(quote(uri.getPath()));
		} else {
			ftp.rm(quote(uri.getPath()));
		}
	}

	private void delete(ChannelSftp ftp, URI uri, DeleteParameters params) throws IOException, SftpException {
		Info info = info(uri, ftp);
		if (info == null) {
			throw new FileNotFoundException(uri.toString());
		}
		delete(ftp, info, params);
	}
	
	@Override
	public boolean delete(SingleCloverURI target, DeleteParameters params) throws IOException {
		URI uri = target.toURI();
		ChannelSftp ftp = null;
		try {
			ftp = connect(uri);
			delete(ftp, uri, params);
			return true;
		} catch (SftpException ex) {
			throw new IOException(ex);
		} finally {
			disconnect(ftp);
		}
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI wildcards,
			ResolveParameters params) throws IOException {
		return manager.defaultResolve(wildcards);
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
		if (userInfo == null) return new String[] {""};
		return decodeString(userInfo).split(":");
	}

	private ChannelSftp connect(URI uri) throws IOException {
		String[] user = getUserInfo(uri);
		Session session = null;
		ChannelSftp channel = null;
		try {
			if (uri.getPort() == 0) session = jsch.getSession(user[0], uri.getHost());
			else session = jsch.getSession(user[0], uri.getHost(), uri.getPort() == -1 ? 22 : uri.getPort());

			// password will be given via UserInfo interface.
			UserInfo aUserInfo = new URLUserInfoIteractive(user.length == 2 ? user[1] : null);
			session.setUserInfo(aUserInfo);
			//if (proxy != null) session.setProxy(proxy);
			session.connect();
			channel = (ChannelSftp) session.openChannel(uri.getScheme());
			channel.connect();
			return channel;
		} catch (Exception e) {
			if (session != null && session.isConnected()) {
				session.disconnect();
			}
			if (channel != null && channel.isConnected()) {
				channel.disconnect();
			}
			throw new IOException(FileOperationMessages.getString("FTPOperationHandler.connection_failed"), e); //$NON-NLS-1$
		}
	}

	private class SFTPInfo implements Info {
		
		private final LsEntry file;
		private final URI uri;
		private final URI parent;
		
		private static final int S_IREAD = 00400; // read by owner
		private static final int S_IWRITE= 00200; // write by owner
		private static final int S_IEXEC = 00100; // execute/search by owner
		
		public SFTPInfo(LsEntry file, URI parent) throws UnsupportedEncodingException {
			this.file = file;
			this.parent = parent;
			String name = file.getFilename();
			if (file.getAttrs().isDir() && !name.endsWith(URIUtils.PATH_SEPARATOR)) {
				name = name + URIUtils.PATH_SEPARATOR;
			}
			URI tmp = URIUtils.getChildURI(parent, name);
			this.uri = tmp;
		}

		@Override
		public String getName() {
			return file.getFilename();
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
			return file.getAttrs().isDir();
		}

		@Override
		public boolean isFile() {
			return !isDirectory() && !isLink();
		}
		
		@Override
		public Boolean isLink() {
			return file.getAttrs().isDir();
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
			return new Date(file.getAttrs().getMTime());
		}

		@Override
		public Date getCreated() {
			return null;
		}

		@Override
		public Date getLastAccessed() {
			return new Date(file.getAttrs().getATime());
		}

		@Override
		public Long getSize() {
			return file.getAttrs().getSize();
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
			return (file.getAttrs().getPermissions() & S_IREAD) != 0;
		}

		@Override
		public Boolean canWrite() {
			return (file.getAttrs().getPermissions() & S_IWRITE) != 0;
		}

		@Override
		public Boolean canExecute() {
			return (file.getAttrs().getPermissions() & S_IEXEC) != 0;
		}
		
	}

	private Info info(LsEntry file, URI parent) {
		if (file == null) {
			return null;
		}
		try {
			return new SFTPInfo(file, parent);
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
			case '\\': case '*': case '?':
				sb.append('\\');
			}
			sb.append(c);
		}
		return sb.toString();
	}
	
	private Info info(URI targetUri, ChannelSftp ftp) {
		try {
			URI parentUri = URIUtils.getParentURI(targetUri);
			String fileName = parentUri.relativize(targetUri).toString();
			if (fileName.endsWith(URIUtils.PATH_SEPARATOR)) {
				fileName = fileName.substring(0, fileName.length()-1);
			}
			fileName = URIUtils.urlDecode(fileName);
			SftpATTRS attrs = ftp.stat(quote(targetUri.getPath()));
			if (!attrs.isDir()) {
				@SuppressWarnings("unchecked")
				Vector<LsEntry> files = ftp.ls(quote(targetUri.getPath()));
				if (files != null && !files.isEmpty()) {
					return new SFTPInfo(files.get(0), parentUri);
				}
			} else {
				@SuppressWarnings("unchecked")
				Vector<LsEntry> files = ftp.ls(quote(parentUri.getPath()));
				if (files != null) {
					for (LsEntry file: files) {
						if ((file != null) && !file.getFilename().equals(URIUtils.CURRENT_DIR_NAME) && !file.getFilename().equals(URIUtils.PARENT_DIR_NAME)) {
							Info info = info(file, parentUri);
							if (info.getName().equals(fileName)) {
								return info;
							}
						}
					}
				}
			}
		} catch (SftpException sftpe) {
			if (sftpe.id != ChannelSftp.SSH_FX_NO_SUCH_FILE) { // other than No such file
				sftpe.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private void disconnect(ChannelSftp ftp) {
		if ((ftp != null) && ftp.isConnected()) {
			try {
				ftp.disconnect();
			} catch(Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	
	private List<Info> list(URI parentUri, ChannelSftp ftp, ListParameters params) throws IOException, SftpException {
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
			@SuppressWarnings("unchecked")
			Vector<LsEntry> files = ftp.ls(quote(parentUri.getPath()));
			List<Info> result = new ArrayList<Info>(files.size());
			for (LsEntry file: files) {
				if ((file != null) && !file.getFilename().equals(URIUtils.CURRENT_DIR_NAME) && !file.getFilename().equals(URIUtils.PARENT_DIR_NAME)) {
					Info child = info(file, parentUri);
					result.add(child);
					if (params.isRecursive() && file.getAttrs().isDir()) {
						result.addAll(list(child.getURI(), ftp, params));
					}
				}
			}
			return result;
		}
	}

	@Override
	public List<Info> list(SingleCloverURI parent, ListParameters params)
			throws IOException {
		URI parentUri = parent.toURI();
		ChannelSftp ftp = connect(parentUri);
		try {
			ftp = connect(parentUri);
			return list(parentUri, ftp, params);
		} catch (SftpException ex) {
			throw new IOException(ex);
		} finally {
			disconnect(ftp);
		}
	}
	
	@Override
	public Info info(SingleCloverURI target, InfoParameters params)
			throws IOException {
		return info(target.toURI());
	}

	private Info info(URI targetUri) throws IOException {
		ChannelSftp ftp = null;
		try {
			ftp = connect(targetUri);
			return info(targetUri, ftp);
		} finally {
			disconnect(ftp);
		}
	}

	private void createFile(ChannelSftp ftp, String path) throws SftpException {
		ftp.put(new ByteArrayInputStream(new byte[0]), path);
	}

	private void create(ChannelSftp ftp, URI uri, CreateParameters params) throws IOException, SftpException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		boolean createDirectory = Boolean.TRUE.equals(params.isDirectory());
		boolean createParents = Boolean.TRUE.equals(params.isMakeParents());
		Info fileInfo = info(uri);
		String path = quote(uri.getPath());
		if (fileInfo == null) { // does not exist
			if (createParents) {
				URI parentUri = URIUtils.getParentURI(uri);
				create(ftp, parentUri, params.clone().setDirectory(true));
			}
			if (createDirectory) {
				ftp.mkdir(path);
			} else {
				createFile(ftp, path);
			}
		} else {
			if (createDirectory != fileInfo.isDirectory()) {
				throw new IOException(MessageFormat.format(createDirectory ? FileOperationMessages.getString("IOperationHandler.exists_not_directory") : FileOperationMessages.getString("IOperationHandler.exists_not_file"), uri)); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
		Date lastModified = params.getLastModified();
		if (lastModified != null) {
			ftp.setMtime(path, (int) lastModified.getTime());
		}
	}

	@Override
	public boolean create(SingleCloverURI target, CreateParameters params)
			throws IOException {
		URI uri = target.toURI();
		ChannelSftp ftp = null;
		try {
			ftp = connect(uri);
			create(ftp, uri, params);
			return true;
		} catch (SftpException ex) {
			throw new IOException(ex);
		} finally {
			disconnect(ftp);
		}
	}

	@Override
	public File getFile(SingleCloverURI uri, FileParameters params)
			throws IOException {
		throw new UnsupportedOperationException();
	}

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
		return "SFTPOperationHandler"; //$NON-NLS-1$
	}

	private class SFTPContent implements Content {
		
		private final URI uri;

		public SFTPContent(URI uri) {
			this.uri = uri;
		}

		@Override
		public ReadableByteChannel read() throws IOException {
			try {
				return Channels.newChannel(connect(uri).get(quote(uri.getPath())));
			} catch (SftpException e) {
				throw new IOException(e);
			}
		}

		@Override
		public WritableByteChannel write() throws IOException {
			try {
				return Channels.newChannel(connect(uri).put(quote(uri.getPath()), ChannelSftp.OVERWRITE));
			} catch (SftpException e) {
				throw new IOException(e);
			}
		}

		@Override
		public WritableByteChannel append() throws IOException {
			try {
				return Channels.newChannel(connect(uri).put(quote(uri.getPath()), ChannelSftp.APPEND));
			} catch (SftpException e) {
				throw new IOException(e);
			}
		}
		
	}
}
