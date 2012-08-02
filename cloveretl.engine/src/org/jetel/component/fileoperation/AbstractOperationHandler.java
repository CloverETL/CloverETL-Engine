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

	protected boolean copyInternal(URI sourceUri, URI targetUri, CopyParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		Info source = simpleHandler.info(sourceUri);
		Info target = simpleHandler.info(targetUri);
		if (source == null) {
			throw new FileNotFoundException(sourceUri.toString());
		}
		if (target != null) {
			if (source.getURI().normalize().equals(target.getURI().normalize())) {
				throw new SameFileException(sourceUri, targetUri);
			}
		}
		if (source.isDirectory()) {
			if (!params.isRecursive()) {
				return false;
			}
			if (target != null && !target.isDirectory()) {
				throw new IOException(format(FileOperationMessages.getString("IOperationHandler.cannot_overwrite_not_a_directory"), source, target)); //$NON-NLS-1$
			}
			if (target == null && !simpleHandler.makeDir(targetUri)) {
				throw new IOException(format(FileOperationMessages.getString("IOperationHandler.create_failed"), target)); //$NON-NLS-1$
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
			}
			return simpleHandler.copyFile(sourceUri, targetUri) != null;
		}
	}
	
	protected SingleCloverURI copy(URI sourceUri, URI targetUri, CopyParameters params) throws IOException {
		Info sourceInfo = simpleHandler.info(sourceUri);
		if (sourceInfo == null) {
			throw new FileNotFoundException(sourceUri.toString());
		}
		Info targetInfo = simpleHandler.info(targetUri);
		if ((targetInfo != null) && targetInfo.isDirectory()) {
			targetUri = URIUtils.getChildURI(targetUri, sourceInfo.getName());
		} else if (!sourceInfo.isDirectory() && targetUri.toString().endsWith(URIUtils.PATH_SEPARATOR)) {
			throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), targetUri)); //$NON-NLS-1$
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
			throw new FileNotFoundException(sourceUri.toString());
		}
		if (target != null) {
			if (source.getURI().normalize().equals(target.getURI().normalize())) {
				throw new SameFileException(sourceUri, targetUri);
			}
		}
		if (source.isDirectory()) {
			if (target != null) {
				if (target.isDirectory()) {
					List<URI> children = simpleHandler.list(targetUri);
					if ((children != null) && (children.size() > 0)) {
						throw new IOException(format(FileOperationMessages.getString("IOperationHandler.not_empty"), target)); //$NON-NLS-1$
					}
					simpleHandler.removeDir(targetUri); // necessary for renameTo()
				} else {
					throw new IOException(format(FileOperationMessages.getString("IOperationHandler.cannot_overwrite_not_a_directory"), source, target)); //$NON-NLS-1$
				}
			}
			if (simpleHandler.renameTo(sourceUri, targetUri) != null) {
				return true;
			}
			if (!simpleHandler.makeDir(targetUri)) {
				throw new IOException(format(FileOperationMessages.getString("IOperationHandler.create_failed"), target)); //$NON-NLS-1$
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
			}
			boolean renamed = simpleHandler.renameTo(sourceUri, targetUri) != null;
			return renamed || (simpleHandler.copyFile(sourceUri, targetUri) != null && simpleHandler.deleteFile(sourceUri));
		}
	}

	protected SingleCloverURI move(URI sourceUri, URI targetUri, MoveParameters params) throws IOException {
		Info sourceInfo = simpleHandler.info(sourceUri);
		if (sourceInfo == null) {
			throw new FileNotFoundException(sourceUri.toString());
		}
		Info targetInfo = simpleHandler.info(targetUri);
		if ((targetInfo != null) && targetInfo.isDirectory()) {
			targetUri = URIUtils.getChildURI(targetUri, sourceInfo.getName());
		} else if (!sourceInfo.isDirectory() && targetUri.toString().endsWith(URIUtils.PATH_SEPARATOR)) {
			throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), targetUri)); //$NON-NLS-1$
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
			throw new FileNotFoundException(target.toString());
		}
		if (!info.isDirectory() && target.toString().endsWith(URIUtils.PATH_SEPARATOR)) {
			throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), target)); //$NON-NLS-1$
		}
		if (params.isRecursive() && info.isDirectory()) {
			for (URI child: simpleHandler.list(target)) {
				delete(child, params);
			}
		}
		return info.isDirectory() ? simpleHandler.removeDir(target) : simpleHandler.deleteFile(target);
	}

	@Override
	public boolean delete(SingleCloverURI target, DeleteParameters params) throws IOException {
		return delete(target.toURI(), params);
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
			throw new FileNotFoundException(uri.toString());
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
		} else {
			if ((isDirectory != null) && (!isDirectory.equals(fileInfo.isDirectory()))) {
				throw new IOException(MessageFormat.format(isDirectory ? FileOperationMessages.getString("IOperationHandler.exists_not_directory") : FileOperationMessages.getString("IOperationHandler.exists_not_file"), uri)); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
		Date lastModified = params.getLastModified();
		if (lastModified != null) {
			success &= simpleHandler.setLastModified(uri, lastModified);
		}
		return success;
	}

	@Override
	public boolean create(SingleCloverURI target, CreateParameters params) throws IOException {
		return create(target.toURI(), params);
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
