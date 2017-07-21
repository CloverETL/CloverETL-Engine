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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 28.3.2012
 */
public abstract class AbstractOperationHandler implements IOperationHandler {
	
	protected final PrimitiveOperationHandler simpleHandler;

	public AbstractOperationHandler(PrimitiveOperationHandler simpleHandler) {
		this.simpleHandler = simpleHandler;
	}

	private static final CreateParameters CREATE_PARENT_DIRS = new CreateParameters().setMakeParents(true).setDirectory(true);
	
	protected boolean copyInternal(URI sourceUri, URI targetUri, CopyParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		Info source = simpleHandler.info(sourceUri);
		Info target = simpleHandler.info(targetUri);
		if (source == null) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), sourceUri.toString())); //$NON-NLS-1$
		}
		if (target != null) {
			if (source.getURI().normalize().equals(target.getURI().normalize())) {
				throw new SameFileException(sourceUri, targetUri);
			}
		}
		boolean makeParents = Boolean.TRUE.equals(params.isMakeParents());
		if (source.isDirectory()) {
			if (!params.isRecursive()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.cannot_copy_directory"), source.getURI())); //$NON-NLS-1$
			}
			if (target != null && !target.isDirectory()) {
				throw new IOException(format(FileOperationMessages.getString("IOperationHandler.cannot_overwrite_not_a_directory"), source.getURI(), target.getURI())); //$NON-NLS-1$
			}
			if (target == null) {
				if (makeParents) {
					URI parentUri = URIUtils.getParentURI(targetUri);
					create(parentUri, CREATE_PARENT_DIRS);
				}
				if (!simpleHandler.makeDir(targetUri)) {
					throw new IOException(format(FileOperationMessages.getString("IOperationHandler.create_failed"), targetUri)); //$NON-NLS-1$
				}
			}
			boolean success = true;
			for (URI child: simpleHandler.list(sourceUri)) {
				Info childInfo = simpleHandler.info(child);
				success &= copyInternal(child, URIUtils.getChildURI(targetUri, childInfo.getName()), params);
			}
			return success;
		} else {
			if (target != null) {
				if (params.isNoOverwrite()) {
					return true;
				}
				if (params.isUpdate() && (source.getLastModified().compareTo(target.getLastModified()) <= 0)) {
					return true;
				}
			} else if (makeParents) {
				URI parentUri = URIUtils.getParentURI(targetUri);
				create(parentUri, CREATE_PARENT_DIRS);
			}
			return simpleHandler.copyFile(sourceUri, targetUri) != null;
		}
	}
	
	protected void checkSubdir(URI source, URI target) throws IOException {
		String sourcePath = source.normalize().toString();
		String targetPath = target.normalize().toString();
		if (!sourcePath.endsWith(URIUtils.PATH_SEPARATOR)) {
			sourcePath = sourcePath + URIUtils.PATH_SEPARATOR;
		}
		if (targetPath.startsWith(sourcePath)) {
			throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.subdirectory"), target, source)); //$NON-NLS-1$
		}
	}
	
	protected SingleCloverURI copy(URI sourceUri, URI targetUri, CopyParameters params) throws IOException {
		Info sourceInfo = simpleHandler.info(sourceUri);
		if (sourceInfo == null) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), sourceUri.toString())); //$NON-NLS-1$
		}
		Info targetInfo = simpleHandler.info(targetUri);
		if ((targetInfo != null) && targetInfo.isDirectory()) {
			targetUri = URIUtils.getChildURI(targetUri, sourceInfo.getName());
		} else if (targetUri.toString().endsWith(URIUtils.PATH_SEPARATOR)) {
			if (Boolean.TRUE.equals(params.isMakeParents())) {
				targetUri = URIUtils.getChildURI(targetUri, sourceInfo.getName());
			} else if (!sourceInfo.isDirectory()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), targetUri)); //$NON-NLS-1$
			}
		}
		if (sourceInfo.isDirectory()) {
			checkSubdir(sourceUri, targetUri);
		}
		return copyInternal(sourceUri, targetUri, params) ? CloverURI.createSingleURI(targetUri) : null;
	}

	@Override
	public SingleCloverURI copy(SingleCloverURI sourceUri, SingleCloverURI targetUri, CopyParameters params)
			throws IOException {
		return copy(sourceUri.toURI(), targetUri.toURI(), params);
	}

	protected boolean moveInternal(URI sourceUri, URI targetUri, MoveParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		Info source = simpleHandler.info(sourceUri);
		Info target = simpleHandler.info(targetUri);
		if (source == null) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), sourceUri.toString())); //$NON-NLS-1$
		}
		if (target != null) {
			if (source.getURI().normalize().equals(target.getURI().normalize())) {
				throw new SameFileException(sourceUri, targetUri);
			}
		}
		boolean makeParents = Boolean.TRUE.equals(params.isMakeParents());
		if (source.isDirectory()) {
			if (target != null) {
				if (target.isDirectory()) {
					List<URI> children = simpleHandler.list(targetUri);
					if ((children != null) && (children.size() > 0)) {
						throw new IOException(format(FileOperationMessages.getString("IOperationHandler.not_empty"), target.getURI())); //$NON-NLS-1$
					}
					simpleHandler.removeDir(targetUri); // necessary for renameTo()
				} else {
					throw new IOException(format(FileOperationMessages.getString("IOperationHandler.cannot_overwrite_not_a_directory"), source.getURI(), target.getURI())); //$NON-NLS-1$
				}
			} else if (makeParents) {
				URI parentUri = URIUtils.getParentURI(targetUri);
				create(parentUri, CREATE_PARENT_DIRS);
			}
			if (simpleHandler.renameTo(sourceUri, targetUri) != null) {
				return true;
			}
			if (!simpleHandler.makeDir(targetUri)) {
				throw new IOException(format(FileOperationMessages.getString("IOperationHandler.create_failed"), target.getURI())); //$NON-NLS-1$
			}
			boolean success = true;
			for (URI child: simpleHandler.list(sourceUri)) {
				Info childInfo = simpleHandler.info(child);
				success &= moveInternal(child, URIUtils.getChildURI(targetUri, childInfo.getName()), params);
			}
			return success && simpleHandler.removeDir(sourceUri);
		} else {
			if (target != null) {
				if (params.isNoOverwrite()) {
					return true;
				}
				if (params.isUpdate() && (source.getLastModified().compareTo(target.getLastModified()) <= 0)) {
					return true;
				}
				simpleHandler.deleteFile(targetUri); // necessary for renameTo()
			} else if (makeParents) {
				URI parentUri = URIUtils.getParentURI(targetUri);
				create(parentUri, CREATE_PARENT_DIRS);
			}
			boolean renamed = simpleHandler.renameTo(sourceUri, targetUri) != null;
			return renamed || (simpleHandler.copyFile(sourceUri, targetUri) != null && simpleHandler.deleteFile(sourceUri));
		}
	}

	protected SingleCloverURI move(URI sourceUri, URI targetUri, MoveParameters params) throws IOException {
		Info sourceInfo = simpleHandler.info(sourceUri);
		if (sourceInfo == null) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), sourceUri.toString())); //$NON-NLS-1$
		}
		Info targetInfo = simpleHandler.info(targetUri);
		if ((targetInfo != null) && targetInfo.isDirectory()) {
			targetUri = URIUtils.getChildURI(targetUri, sourceInfo.getName());
		} else if (targetUri.toString().endsWith(URIUtils.PATH_SEPARATOR)) {
			if (Boolean.TRUE.equals(params.isMakeParents())) {
				targetUri = URIUtils.getChildURI(targetUri, sourceInfo.getName());
			} else if (!sourceInfo.isDirectory()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), targetUri)); //$NON-NLS-1$
			}
		}
		if (sourceInfo.isDirectory()) {
			checkSubdir(sourceUri, targetUri);
		}
		return moveInternal(sourceUri, targetUri, params) ? SingleCloverURI.createSingleURI(targetUri) : null;
	}

	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params)
			throws IOException {
		return move(source.toURI(), target.toURI(), params);
	}

	private class DefaultContent implements Content {
		
		private final URI uri;
		
		public DefaultContent(URI uri) {
			this.uri = uri;
		}

		@Override
		public ReadableByteChannel read() throws IOException {
			return simpleHandler.read(uri);
		}
		
		public WritableByteChannel write(boolean append) throws IOException {
			Info info = simpleHandler.info(uri);
			if (info == null) {
				simpleHandler.createFile(uri);
			}
			return append ? simpleHandler.append(uri) : simpleHandler.write(uri);
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
	public ReadableContent getInput(SingleCloverURI source, ReadParameters params) throws IOException {
		return new DefaultContent(source.toURI());
	}

	@Override
	public WritableContent getOutput(SingleCloverURI target, WriteParameters params) throws IOException {
		return new DefaultContent(target.toURI());
	}
	
	protected boolean delete(URI target, DeleteParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		Info info = simpleHandler.info(target);
		if (info == null) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), target.toString())); //$NON-NLS-1$
		}
		if (!info.isDirectory() && target.toString().endsWith(URIUtils.PATH_SEPARATOR)) {
			throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), target)); //$NON-NLS-1$
		}
		if (info.isDirectory()) {
			if (params.isRecursive()) {
				for (URI child: simpleHandler.list(target)) {
					delete(child, params);
				}
			} else {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.cannot_remove_directory"), target)); //$NON-NLS-1$
			}
		}
		return info.isDirectory() ? simpleHandler.removeDir(target) : simpleHandler.deleteFile(target);
	}

	@Override
	public SingleCloverURI delete(SingleCloverURI target, DeleteParameters params) throws IOException {
		URI uri = target.toURI().normalize();
		if (delete(uri, params)) {
			return CloverURI.createSingleURI(uri);
		}
		return null;
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI uri, ResolveParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	protected List<Info> list(URI uri, ListParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		Info info = simpleHandler.info(uri);
		if (info == null) {
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), uri.toString())); //$NON-NLS-1$
		}
		if (uri.toString().endsWith(URIUtils.PATH_SEPARATOR) && !info.isDirectory()) {
			throw new FileNotFoundException(format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), uri)); //$NON-NLS-1$
		}
		if (info.isDirectory()) {
			List<URI> children = simpleHandler.list(uri); 
			List<Info> result = new ArrayList<Info>(children.size());
			for (URI child: children) {
				Info childInfo = simpleHandler.info(child);
				result.add(childInfo);
				if (params.isRecursive() && childInfo.isDirectory()) {
					result.addAll(list(child, params));
				}
			}
			return result;
		} else {
			return Arrays.asList(info);
		}
	}

	@Override
	public List<Info> list(SingleCloverURI parent, ListParameters params) throws IOException {
		return list(parent.toURI(), params);
	}

	@Override
	public Info info(SingleCloverURI target, InfoParameters params) throws IOException {
		return simpleHandler.info(target.toURI());
	}
	
	protected boolean create(URI uri, CreateParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		Boolean isDirectory = params.isDirectory();
		boolean createDirectory = Boolean.TRUE.equals(isDirectory);
		boolean createParents = Boolean.TRUE.equals(params.isMakeParents());
		Date lastModified = params.getLastModified();
		Info fileInfo = simpleHandler.info(uri);
		boolean success = true;
		if (fileInfo == null) { // does not exist
			if (createParents) {
				URI parentUri = URIUtils.getParentURI(uri);
				create(parentUri, params.clone().setDirectory(true));
			}
			if (createDirectory) {
				success = simpleHandler.makeDir(uri);
			} else {
				success = simpleHandler.createFile(uri);
			}
			if (lastModified != null) {
				success &= simpleHandler.setLastModified(uri, lastModified);
			}
		} else {
			if ((isDirectory != null) && (!isDirectory.equals(fileInfo.isDirectory()))) {
				throw new IOException(MessageFormat.format(isDirectory ? FileOperationMessages.getString("IOperationHandler.exists_not_directory") : FileOperationMessages.getString("IOperationHandler.exists_not_file"), uri)); //$NON-NLS-1$ //$NON-NLS-2$
			}
			if (lastModified != null) {
				success &= simpleHandler.setLastModified(uri, lastModified);
			} else {
				simpleHandler.setLastModified(uri, new Date());
			}
		}
		return success;
	}

	@Override
	public SingleCloverURI create(SingleCloverURI target, CreateParameters params) throws IOException {
		URI uri = target.toURI().normalize();
		if (create(uri, params)) {
			return CloverURI.createSingleURI(uri);
		}
		return null;
	}

	@Override
	public File getFile(SingleCloverURI uri, FileParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return String.format("AbstractOperationHandler (%s)", simpleHandler); //$NON-NLS-1$
	}
	
	

}
