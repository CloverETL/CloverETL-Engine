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
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

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
import org.jetel.util.file.FileUtils;

public class LocalOperationHandler implements IOperationHandler {
	
	static final String FILE_SCHEME = "file"; //$NON-NLS-1$
	
	public static final int PRIORITY = Integer.MAX_VALUE - 100;
	
	@Override
	public int getPriority(Operation operation) {
		return PRIORITY;
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
				return operation.scheme().equalsIgnoreCase(FILE_SCHEME);
			case COPY:
			case MOVE:
				return operation.scheme(0).equalsIgnoreCase(FILE_SCHEME)
						&& operation.scheme(1).equalsIgnoreCase(FILE_SCHEME);
			default: 
				return false;
		}
	}
	
	private boolean copyInternal(File source, File target, CopyParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		try {
			source = source.getCanonicalFile();
		} catch (IOException ioe) {
			// ignore the exception, it's just an attempt
//			ioe.printStackTrace();
		}
		try {
			target = target.getCanonicalFile();
		} catch (IOException ioe) {
			// ignore the exception, it's just an attempt
//			ioe.printStackTrace();
		}
		if (source.equals(target)) {
			throw new SameFileException(source, target);
		}
		if (source.isDirectory()) {
			if (!params.isRecursive()) {
				return false;
			}
			if (target.exists() && !target.isDirectory()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.cannot_overwrite_not_a_directory"), source, target)); //$NON-NLS-1$
			}
			if (!target.exists() && !target.mkdir()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.create_failed"), target)); //$NON-NLS-1$
			}
			boolean success = true;
			for (File child: source.listFiles()) {
				success &= copyInternal(child, new File(target, child.getName()), params);
			}
			return success;
		} else {
			if (target.exists()) {
				if (params.isNoOverwrite()) {
					return true;
				}
				if (params.isUpdate() && (source.lastModified() <= target.lastModified())) {
					return true;
				}
			} else {
				if (!target.createNewFile()) {
					throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.create_failed"), target)); //$NON-NLS-1$
				}
			}
			return FileUtils.copyFile(source, target);
		}
	}

	private SingleCloverURI copy(File source, File target, CopyParameters params) throws IOException {
		if (!source.exists()) {
			throw new FileNotFoundException(source.toString());
		}
		if (target.isDirectory()) {
			target = new File(target, source.getName());
		}
		return copyInternal(source, target, params) ? CloverURI.createSingleURI(target.toURI()) : null;
	}

	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target, CopyParameters params) throws IOException {
		return copy(new File(source.toURI()), new File(target.toURI()), params);
	}
	
	private SingleCloverURI move(File source, File target, MoveParameters params) throws IOException {
		if (!source.exists()) {
			throw new FileNotFoundException(source.toString());
		}
		if (target.isDirectory()) {
			target = new File(target, source.getName());
		}
		return moveInternal(source, target, params) ? SingleCloverURI.createSingleURI(target.toURI()) : null;
	}
	
	private boolean moveInternal(File source, File target, MoveParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		try {
			source = source.getCanonicalFile();
		} catch (IOException ioe) {
			// ignore the exception, it's just an attempt
//			ioe.printStackTrace();
		}
		try {
			target = target.getCanonicalFile();
		} catch (IOException ioe) {
			// ignore the exception, it's just an attempt
//			ioe.printStackTrace();
		}
		if (source.equals(target)) {
			throw new SameFileException(source, target);
		}
		if (source.isDirectory()) {
			if (target.exists()) {
				if (target.isDirectory()) {
					File[] children = target.listFiles();
					if ((children != null) && (children.length > 0)) {
						throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_empty"), target)); //$NON-NLS-1$
					}
					target.delete(); // necessary for renameTo()
				} else {
					throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.cannot_overwrite_not_a_directory"), source, target)); //$NON-NLS-1$
				}
			}
			if (source.renameTo(target)) {
				return true;
			}
			if (!target.mkdir()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.create_failed"), target)); //$NON-NLS-1$
			}
			boolean success = true;
			for (File child: source.listFiles()) {
				File childTarget = new File(target, child.getName());
				success &= moveInternal(child, childTarget, params);
			}
			return success && source.delete();
		} else {
			if (target.exists()) {
				if (params.isNoOverwrite()) {
					return true;
				}
				if (params.isUpdate() && (source.lastModified() <= target.lastModified())) {
					return true;
				}
				target.delete(); // necessary for renameTo()
			}
			boolean renamed = source.renameTo(target);
			return renamed || (FileUtils.copyFile(source, target) && source.delete());
		}
	}

	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params) throws IOException {
		return move(new File(source.toURI()), new File(target.toURI()), params);
	}
	
	private static class FileContent implements Content {
		
		private final File file;
		
		public FileContent(File file) {
			this.file = file;
		}

		@Override
		public FileChannel read() throws IOException {
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(file);
				return fis.getChannel();
			} catch (Exception ex) { // close the input stream on exception
				FileUtils.closeQuietly(fis);
				if (ex instanceof IOException) {
					throw (IOException) ex;
				} else {
					throw new IOException(ex);
				}
			}
		}
		
		public FileChannel write(boolean append) throws IOException {
			if (!file.exists()) {
				file.createNewFile();
			}
			FileOutputStream fos = null;
			try {
				fos = new FileOutputStream(file, append);
				return fos.getChannel();
			} catch (Exception ex) { // close the output stream on exception
				FileUtils.closeQuietly(fos);
				if (ex instanceof IOException) {
					throw (IOException) ex;
				} else {
					throw new IOException(ex);
				}
			}
		}

		@Override
		public FileChannel write() throws IOException {
			return write(false);
		}

		@Override
		public WritableByteChannel append() throws IOException {
			return write(true);
		}
		
	}
	
	@Override
	public Content getInput(SingleCloverURI source, ReadParameters params) {
		return new FileContent(new File(source.toURI()));
	}

	@Override
	public Content getOutput(SingleCloverURI target, WriteParameters params) {
		return new FileContent(new File(target.toURI()));
	}
	
	private boolean delete(File file, DeleteParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		if (!file.exists()) {
			throw new FileNotFoundException(file.toString());
		}
		if (file.isDirectory()) {
			File[] children = file.listFiles();
			if (params.isRecursive()) {
				for (File child: children) {
					delete(child, params);
				}
			} else {
				if ((children != null) && (children.length > 0)) {
					throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_empty"), file)); //$NON-NLS-1$
				}
			}
		}
		return file.delete();
	}

	@Override
	public boolean delete(SingleCloverURI target, DeleteParameters params) throws IOException {
		return delete(new File(target.toURI()), params);
	}
	
	private List<String> getParts(String uri) {
		return FileManager.getParts(uri);
	}
	
	private boolean hasWildcards(String path) {
		return FileManager.hasWildcards(path);
	}
	
	private static class WildcardFileFilter extends WildcardFilter implements FileFilter {
		
		public WildcardFileFilter(String name, boolean directory) {
			super(name, directory);
		}

		@Override
		public boolean accept(File file) {
			return accept(file.getName(), file.isDirectory());
		}
		
	}
	
	private List<File> expand(File base, String part, boolean directory) {
		if (base == null) {
			throw new NullPointerException("base"); //$NON-NLS-1$
		}
		if (!base.isDirectory()) {
			throw new IllegalArgumentException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), base)); //$NON-NLS-1$
		}
		part = URIUtils.urlDecode(part);
		if (hasWildcards(part)) {
			File[] children = base.listFiles(new WildcardFileFilter(part, directory));
			return children != null ? Arrays.asList(children) : new ArrayList<File>(0); 
		} else {
			File file = new File(base, part);
			if (file.exists()) {
				return Arrays.asList(file);
			} else {
				return new ArrayList<File>(0);
			}
		}
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI wildcards, ResolveParameters params) {
		String uriString = wildcards.toString();
		if (!hasWildcards(uriString)) {
			return Arrays.asList(wildcards);
		}
		
		List<String> parts = getParts(uriString);
		try {
			File base = new File(new URI(parts.get(0)));
			if (!base.exists()) {
				return new ArrayList<SingleCloverURI>(0);
			}
			List<File> bases = Arrays.asList(base);
			for (Iterator<String> it = parts.listIterator(1); it.hasNext(); ) {
				String part = it.next();
				boolean hasPathSeparator = part.endsWith(URIUtils.PATH_SEPARATOR);
				if (hasPathSeparator) {
					part = part.substring(0, part.length()-1);
				}
				List<File> nextBases = new ArrayList<File>(bases.size());
				for (File f: bases) {
					nextBases.addAll(expand(f, part, it.hasNext() || hasPathSeparator));
				}
				bases = nextBases;
			}
			
			List<SingleCloverURI> result = new ArrayList<SingleCloverURI>(bases.size());
			for (File file: bases) {
				result.add(new SingleCloverURI(file.toURI()));
			}
			return result;
		} catch (URISyntaxException ex) {
			ex.printStackTrace(); // FIXME
		}
		
		return null;
	}
	
	private List<Info> list(File parent, ListParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		if (!parent.exists()) {
			throw new FileNotFoundException(parent.toString());
		}
		if (!parent.isDirectory()) {
			return Arrays.asList(info(parent));
		}
		File[] children = parent.listFiles();
		if (children != null) {
			List<Info> result = new ArrayList<Info>();
			for (File child: children) {
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
		File file = new File(uri);
		if (uri.toString().endsWith(URIUtils.PATH_SEPARATOR) && !file.isDirectory()) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), file)); //$NON-NLS-1$
		}
		return list(file, params);
	}
	
	private boolean create(File file, CreateParameters params) throws IOException {
		boolean success = true;
		Boolean isDirectory = params.isDirectory();
		boolean createParents = Boolean.TRUE.equals(params.isMakeParents()); 
		if (!file.exists()) {
			boolean createDirectory = Boolean.TRUE.equals(isDirectory);
			if (createDirectory && createParents) {
				success = file.mkdirs(); // directory and parents
			} else if (createDirectory) {
				success = file.mkdir(); // directory
			} else { // file
				if (createParents) {
					File parentDir = file.getParentFile();
					if (parentDir != null && !parentDir.isDirectory()) {
						parentDir.mkdirs();
					}
				}
				success = file.createNewFile();
			}
		}
		Date lastModified = params.getLastModified();
		if (lastModified != null) {
			success &= file.setLastModified(lastModified.getTime());
		}
		if ((isDirectory != null) && (!isDirectory.equals(file.isDirectory()))) {
			throw new IOException(MessageFormat.format(isDirectory ? FileOperationMessages.getString("IOperationHandler.exists_not_directory") : FileOperationMessages.getString("IOperationHandler.exists_not_file"), file)); //$NON-NLS-1$ //$NON-NLS-2$
		}
		return success; 
	}

	@Override
	public boolean create(SingleCloverURI target, CreateParameters params) throws IOException {
		return create(new File(target.toURI()), params);
	}
	
	private Info info(File file) {
		if (file.exists()) {
			return new FileInfo(file);
		}
		return null;
	}

	@Override
	public Info info(SingleCloverURI target, InfoParameters params) {
		return info(new File(target.toURI()));
	}
	
	@Override
	public File getFile(SingleCloverURI uri, FileParameters params) throws IOException {
		return new File(uri.toURI());
	}

	@Override
	public String toString() {
		return "LocalOperationHandler"; //$NON-NLS-1$
	}
	
	

}
