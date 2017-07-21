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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import jcifs.Config;
import jcifs.smb.NtStatus;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;

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
import org.jetel.exception.JetelRuntimeException;

public class SMBOperationHandler implements IOperationHandler {
	
	static final String SMB_SCHEME = "smb"; //$NON-NLS-1$
	private static final String SMB_ROOT_URL = SMB_SCHEME + "://"; //$NON-NLS-1$
	
	static {
		Config.setProperty("jcifs.smb.client.ignoreCopyToException", "false");
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
			case FILE:
				return operation.scheme().equalsIgnoreCase(SMB_SCHEME);
			case COPY:
			case MOVE:
				return operation.scheme(0).equalsIgnoreCase(SMB_SCHEME)
						&& operation.scheme(1).equalsIgnoreCase(SMB_SCHEME);
			default: 
				return false;
		}
	}
	
	private void checkSubdir(SmbFile sourceDir, SmbFile targetDir) throws IOException {
		sourceDir = ensureTrailingSlash(sourceDir);
		targetDir = ensureTrailingSlash(targetDir);
		String parentDir = targetDir.getCanonicalPath();
		while (!SMB_ROOT_URL.equals(parentDir)) {
			SmbFile parent = new SmbFile(parentDir);
			if (sourceDir.equals(parent)) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.subdirectory"), targetDir, sourceDir)); //$NON-NLS-1$
			}
			parentDir = parent.getParent();
		}
	}
	
	private static SmbFile toFile(URI fileUri) throws MalformedURLException {
		return new SmbFile(decodeURI(fileUri));
	}

	public static String decodeURI(URI fileUri) {
		 // decode %-encoding in everything except authority
		StringBuilder sb = new StringBuilder();
		if (fileUri.getScheme() != null) {
			sb.append(fileUri.getScheme()).append("://");
		}
		if (fileUri.getRawAuthority() != null) {
			sb.append(fileUri.getRawAuthority());
		}
		sb.append(fileUri.getPath());
		if (fileUri.getQuery() != null) {
			sb.append('?').append(fileUri.getQuery());
		}
		if (fileUri.getFragment() != null) {
			sb.append('#').append(fileUri.getFragment());
		}
		return sb.toString();
	}
	
	private static SmbFile toCannonicalFile(URI fileUri) throws MalformedURLException {
		return toCannonicalFile(toFile(fileUri));
	}

	private static SmbFile toCannonicalFile(SmbFile file) {
		try {
			return new SmbFile(file.getCanonicalPath());
		} catch (MalformedURLException ex) {
			// ignore, shouldn't happen
			ex.printStackTrace();
		}
		return file;
	}
	
	private void copyInternal(SmbFile source, SmbFile target, CopyParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		source = toCannonicalFile(source);
		target = toCannonicalFile(target);
		if (source.equals(target)) {
			throw new SameFileException(source, target);
		}
		boolean makeParents = Boolean.TRUE.equals(params.isMakeParents());
		if (source.isDirectory()) {
			if (!params.isRecursive()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.cannot_copy_directory"), source)); //$NON-NLS-1$
			}
			if (target.exists() && !target.isDirectory()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.cannot_overwrite_not_a_directory"), source, target)); //$NON-NLS-1$
			}
			source = ensureTrailingSlash(source);
			target = ensureTrailingSlash(target);
			if (!target.exists()) {
				try {
					if (makeParents) {
						target.mkdirs();
					} else {
						target.mkdir();
					}
				} catch (SmbException e) {
					throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.create_failed"), target), e); //$NON-NLS-1$
				}
				source.copyTo(target);
			} else {
				for (SmbFile child: source.listFiles()) {
					copyInternal(child, new SmbFile(target, child.getName()), params);
				}
			}
		} else {
			if (target.exists()) {
				if (params.isNoOverwrite()) {
					return;
				}
				if (params.isUpdate() && (source.lastModified() <= target.lastModified())) {
					return;
				}
			} else if (makeParents) {
				mkParentDirs(target);
			}

			source.copyTo(target);
		}
	}

	private static void mkParentDirs(SmbFile target) throws MalformedURLException, SmbException {
		SmbFile parent = new SmbFile(target.getParent());
		if (!parent.exists()) {
			parent.mkdirs();
		}
	}

	private static SmbFile ensureTrailingSlash(SmbFile dir) throws MalformedURLException {
		if (!dir.getPath().endsWith("/")) {
			dir = new SmbFile(dir.getPath() + "/");
		}
		return dir;
	}

	private SingleCloverURI copy(URI sourceUri, URI targetUri, CopyParameters params) throws IOException {
		SmbFile source = toCannonicalFile(sourceUri);
		if (!source.exists()) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), source.toString())); //$NON-NLS-1$
		}
		SmbFile target = toCannonicalFile(targetUri);
		if (target.isDirectory()) {
			target = new SmbFile(ensureTrailingSlash(target), source.getName());
		} else if (targetUri.toString().endsWith(URIUtils.PATH_SEPARATOR)) {
			if (Boolean.TRUE.equals(params.isMakeParents())) {
				target = new SmbFile(ensureTrailingSlash(target), source.getName());
			} else if (!source.isDirectory()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), targetUri)); //$NON-NLS-1$
			}
		}
		if (source.isDirectory()) {
			checkSubdir(source, target);
		}
		copyInternal(source, target, params);
		return createSingleCloverURI(target);
	}

	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target, CopyParameters params) throws IOException {
		return copy(source.toURI(), target.toURI(), params);
	}
	
	private SingleCloverURI move(URI sourceUri, URI targetUri, MoveParameters params) throws IOException {
		SmbFile source = toCannonicalFile(sourceUri);
		if (!source.exists()) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), source.toString())); //$NON-NLS-1$
		}
		SmbFile target = toCannonicalFile(targetUri);
		if (target.isDirectory()) {
			target = new SmbFile(ensureTrailingSlash(target), source.getName());
		} else if (targetUri.toString().endsWith(URIUtils.PATH_SEPARATOR)) {
			if (Boolean.TRUE.equals(params.isMakeParents())) {
				target = new SmbFile(ensureTrailingSlash(target), source.getName());
			} else if (!source.isDirectory()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), targetUri)); //$NON-NLS-1$
			}
		}
		if (source.isDirectory()) {
			checkSubdir(source, target);
		}
		
		if (moveInternal(source, target, params)) {
			return createSingleCloverURI(target);
		}
		return null;
	}

	private boolean moveInternal(SmbFile source, SmbFile target, MoveParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		source = toCannonicalFile(source);
		target = toCannonicalFile(target);
		if (source.equals(target)) {
			throw new SameFileException(source, target);
		}
		boolean makeParents = Boolean.TRUE.equals(params.isMakeParents());
		if (source.isDirectory()) {
			source = ensureTrailingSlash(source);
			target = ensureTrailingSlash(target);
			if (target.exists()) {
				if (target.isDirectory()) {
					SmbFile[] children = target.listFiles();
					if ((children != null) && (children.length > 0)) {
						throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_empty"), target)); //$NON-NLS-1$
					}
					target.delete(); // necessary for renameTo()
				} else {
					throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.cannot_overwrite_not_a_directory"), source, target)); //$NON-NLS-1$
				}
			} else if (makeParents) {
				mkParentDirs(target);
			}
			
			try {
				source.renameTo(target);
				return true;
			} catch (SmbException e) {
			}
			
			boolean success = true;
			try {
				target.mkdir();
			} catch (SmbException e) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.create_failed"), target), e); //$NON-NLS-1$
			}
			for (SmbFile child: source.listFiles()) {
				SmbFile childTarget = new SmbFile(target, child.getName());
				success &= moveInternal(child, childTarget, params);
			}
			source.delete();
			return success;
		} else {
			if (target.exists()) {
				if (params.isNoOverwrite()) {
					return true;
				}
				if (params.isUpdate() && (source.lastModified() <= target.lastModified())) {
					return true;
				}
				target.delete(); // necessary for renameTo()
			} else if (makeParents) {
				mkParentDirs(target);
			}
			
			try {
				source.renameTo(target);
				return true;
			} catch (SmbException e) {
			}
			
			source.copyTo(target);
			source.delete();
			return true;
		}
	}

	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params) throws IOException {
		return move(source.toURI(), target.toURI(), params);
	}
	
	private static class FileContent implements Content {
		
		private final SmbFile file;
		
		public FileContent(SmbFile file) {
			this.file = toCannonicalFile(file);
		}

		@Override
		public ReadableByteChannel read() throws IOException {
			return Channels.newChannel(file.getInputStream());
		}
		
		public WritableByteChannel write(boolean append) throws IOException {
			return Channels.newChannel(new SmbFileOutputStream(file, append));
		}

		@Override
		public WritableByteChannel write() throws IOException {
			return write(false);
		}

		@Override
		public WritableByteChannel append() throws IOException {
			return write(true);
		}
		
	}
	
	@Override
	public Content getInput(SingleCloverURI source, ReadParameters params) throws MalformedURLException {
		return new FileContent(toCannonicalFile(source.toURI()));
	}

	@Override
	public Content getOutput(SingleCloverURI target, WriteParameters params) throws MalformedURLException {
		return new FileContent(toCannonicalFile(target.toURI()));
	}
	
	private boolean delete(SmbFile file, DeleteParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		if (!file.exists()) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), file.toString())); //$NON-NLS-1$
		}
		if (file.isDirectory()) {
			if (!params.isRecursive()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.cannot_remove_directory"), file)); //$NON-NLS-1$
			}
			file = ensureTrailingSlash(file);
		}
		file.delete();
		return true;
	}

	@Override
	public SingleCloverURI delete(SingleCloverURI target, DeleteParameters params) throws IOException {
		URI uri = target.toURI();
		SmbFile file = toCannonicalFile(uri);
		if (uri.toString().endsWith(URIUtils.PATH_SEPARATOR) && !file.isDirectory()) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), file)); //$NON-NLS-1$
		}
		if (delete(file, params)) {
			return createSingleCloverURI(file);
		}
		return null;
	}
	
	private static List<SmbFile> expand(SmbFile base, String part, boolean directory) throws IOException {
		if (base == null) {
			throw new NullPointerException("base"); //$NON-NLS-1$
		}
		if (!base.isDirectory()) {
			throw new IllegalArgumentException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), base)); //$NON-NLS-1$
		}
		
		base = ensureTrailingSlash(base);
		part = URIUtils.urlDecode(part);
		if (hasWildcards(part)) {
			SmbFile[] children;
			try {
				// Let's use cool SmbFile.listFiles(String) method which resolves exactly our (DOS) wildcards...
				children = base.listFiles(part);
			} catch (SmbException e) {
				// ... but it uses not so cool way of letting know that there are no files matching the pattern 
				if (e.getNtStatus() == NtStatus.NT_STATUS_NO_SUCH_FILE) {
					return Collections.emptyList(); // maybe this could be done on any SmbException
				} else {
					throw e;
				}
			}
			if (!directory) {
				return Arrays.asList(children);
			} else {
				List<SmbFile> dirsOnly = new ArrayList<SmbFile>(children.length);
				for (SmbFile child : children) {
					if (child.isDirectory()) {
						dirsOnly.add(child);
					}
				}
				return dirsOnly;
			}
		} else {
			SmbFile file = new SmbFile(base, part);
			if (file.exists()) {
				return Arrays.asList(file);
			} else {
				return new ArrayList<SmbFile>(0);
			}
		}
	}

	private static List<String> getParts(String uri) {
		return FileManager.getUriParts(uri);
	}
	
	private static boolean hasWildcards(String path) {
		return FileManager.uriHasWildcards(path);
	}
	
	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI wildcards, ResolveParameters params) throws IOException {
		List<SmbFile> bases = resolve(decodeURI(wildcards.toURI()));
		List<SingleCloverURI> result = new ArrayList<SingleCloverURI>(bases.size());
		for (SmbFile file: bases) {
			result.add(createSingleCloverURI(file));
		}
		return result;
	}
	
	public static List<SmbFile> resolve(String wildcardURL) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		if (!hasWildcards(wildcardURL)) {
			return Arrays.asList(new SmbFile(wildcardURL));
		}

		List<String> parts = getParts(wildcardURL);
		SmbFile base = new SmbFile(parts.get(0));
		if (!base.exists()) {
			return Collections.emptyList();
		}
		List<SmbFile> bases = Arrays.asList(base);
		for (Iterator<String> it = parts.listIterator(1); it.hasNext(); ) {
			String part = it.next();
			boolean hasPathSeparator = part.endsWith(URIUtils.PATH_SEPARATOR);
			if (hasPathSeparator) {
				part = part.substring(0, part.length()-1);
			}
			List<SmbFile> nextBases = new ArrayList<SmbFile>(bases.size());
			for (SmbFile f: bases) {
				nextBases.addAll(expand(f, part, it.hasNext() || hasPathSeparator));
			}
			bases = nextBases;
		}
		return bases;
	}
	
	private List<Info> list(SmbFile parent, ListParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		if (!parent.exists()) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), parent.toString())); //$NON-NLS-1$
		}
		if (!parent.isDirectory()) {
			return Arrays.asList(info(parent));
		}
		SmbFile[] children = ensureTrailingSlash(parent).listFiles();
		if (children != null) {
			List<Info> result = new ArrayList<Info>();
			for (SmbFile child: children) {
				result.add(info(child));
				if (params.isRecursive() && child.isDirectory()) {
					result.addAll(list(child, params));
				}
			}
			return result;
		}
		return new ArrayList<Info>(0);
	}

	@Override
	public List<Info> list(SingleCloverURI parent, ListParameters params) throws IOException {
		URI uri = parent.toURI();
		SmbFile file = toCannonicalFile(uri);
		if (uri.toString().endsWith(URIUtils.PATH_SEPARATOR) && !file.isDirectory()) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), file)); //$NON-NLS-1$
		}
		return list(file, params);
	}
	
	private boolean create(SmbFile file, CreateParameters params) throws IOException {
		boolean success = true;
		Boolean isDirectory = params.isDirectory();
		boolean createParents = Boolean.TRUE.equals(params.isMakeParents()); 
		Date lastModified = params.getLastModified();
		if (!file.exists()) {
			boolean createDirectory = Boolean.TRUE.equals(isDirectory);
			if (createDirectory && createParents) {
				file.mkdirs(); // directory and parents
			} else if (createDirectory) {
				file.mkdir(); // directory
			} else { // file
				if (createParents) {
					mkParentDirs(file);
				}
				file.createNewFile();
			}
			if (lastModified != null) {
				file.setLastModified(lastModified.getTime());
			}
		} else {
			if ((isDirectory != null) && (!isDirectory.equals(file.isDirectory()))) {
				throw new IOException(MessageFormat.format(isDirectory ? FileOperationMessages.getString("IOperationHandler.exists_not_directory") : FileOperationMessages.getString("IOperationHandler.exists_not_file"), file)); //$NON-NLS-1$ //$NON-NLS-2$
			}
			if (lastModified != null) {
				file.setLastModified(lastModified.getTime());
			} else {
				file.setLastModified(System.currentTimeMillis());
			}
		}
		return success; 
	}

	@Override
	public SingleCloverURI create(SingleCloverURI target, CreateParameters params) throws IOException {
		SmbFile file = toCannonicalFile(target.toURI());
		if (create(file, params)) {
			return createSingleCloverURI(file);
		}
		return null;
	}

	private static SingleCloverURI createSingleCloverURI(SmbFile file) {
		return CloverURI.createSingleURI(toURI(file));
	}

	static URI toURI(SmbFile file) {
		URL url = file.getURL(); // may contain spaces in path
		try {
			// %-encode path, query and fragment...
			URI uri = new URI(url.getProtocol(), null, url.getPath(), url.getQuery(), url.getRef());

			// ... but do not %-encode authority (use authority from URL, everything else from URI)
			StringBuilder sb = new StringBuilder();
			sb.append(uri.getScheme()).append("://").append(url.getAuthority()).append(uri.getRawPath());
			if (uri.getRawQuery() != null) {
				sb.append('?').append(uri.getRawQuery());
			}
			if (uri.getRawFragment() != null) {
				sb.append('#').append(uri.getRawFragment());
			}
			
			return new URI(sb.toString());
		} catch (URISyntaxException e) {
			// shouldn't happen
			throw new JetelRuntimeException(e);
		}
	}
	
	private Info info(SmbFile file) throws SmbException {
		if (file.exists()) {
			return new SMBFileInfo(file);
		}
		return null;
	}

	@Override
	public Info info(SingleCloverURI target, InfoParameters params) throws IOException {
		return info(toFile(target.toURI()));
	}
	
	@Override
	public File getFile(SingleCloverURI uri, FileParameters params) throws IOException {
		throw new UnsupportedOperationException(); // this method isn't used anyway (if I'm not blind)
		/*
		The following should work, but is it a good idea to mix handling of SMB using both jcifs.smb.SmbFile and java.io.File?
		URI smbUri = uri.toURI();
		if (smbUri.getUserInfo() == null) {
			try {
				// TODO refactor & TEST!!! all those shitty possibilities
				String ssp = "////" + smbUri.getAuthority() + smbUri.getRawPath() + (smbUri.getRawQuery() != null ? smbUri.getRawQuery() : "");
				URI fileUri = new URI("file", ssp, smbUri.getRawFragment());
				return new File(fileUri);
			} catch (URISyntaxException e) {
				throw new IOException(e);
			}
		}
		throw new MalformedURLException("Cannot convert smb:// URL with nonempty user info to java.io.File");
		*/
	}

	@Override
	public String toString() {
		return "SMBOperationHandler"; //$NON-NLS-1$
	}
	
	

}
