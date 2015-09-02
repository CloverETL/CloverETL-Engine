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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.util.List;

import org.jetel.component.fileoperation.SimpleParameters.CopyParameters;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.SimpleParameters.MoveParameters;
import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 3. 2015
 */
public class S3OperationHandler extends AbstractOperationHandler {
	
	public static final String S3_SCHEME = "s3";
	
	private final PrimitiveS3OperationHandler s3handler;

	private FileManager manager = FileManager.getInstance();

	public S3OperationHandler() {
		super(new PrimitiveS3OperationHandler());
		this.s3handler = (PrimitiveS3OperationHandler) simpleHandler;
	}

	protected S3OperationHandler(PrimitiveS3OperationHandler handler) {
		super(handler);
		this.s3handler = (PrimitiveS3OperationHandler) simpleHandler;
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
			return operation.scheme().equalsIgnoreCase(S3_SCHEME);
		case COPY:
		case MOVE:
			return operation.scheme(0).equalsIgnoreCase(S3_SCHEME)
					&& operation.scheme(1).equalsIgnoreCase(S3_SCHEME);
		default:
			return false;
		}
	}

	@Override
	protected boolean create(URI uri, CreateParameters params) throws IOException {
		String uriString = uri.toString();
		if (uriString.endsWith("/")) {
			uriString = uriString.substring(0, uriString.length() - 1);
			uri = URI.create(uriString);
			params.setDirectory(true);
		}
		return super.create(uri, params);
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI uri, ResolveParameters params) throws IOException {
		return manager.defaultResolve(uri);
	}

	/*
	 * Overridden to delegate moving between different connections
	 * to DefaultOperationHandler.
	 */
	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target, CopyParameters params)
			throws IOException {
		URI sourceUri = source.toURI();
		URI targetUri = target.toURI();
		if (sourceUri.getAuthority().equals(targetUri.getAuthority())) {
			return super.copy(sourceUri, targetUri, params);
		}

		IOperationHandler nextHandler = manager.findNextHandler(Operation.move(source.getScheme(), target.getScheme()), this);
		if (nextHandler != null) {
			return nextHandler.copy(source, target, params);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/*
	 * Overridden to delegate moving between different connections
	 * to DefaultOperationHandler.
	 */
	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params)
			throws IOException {
		URI sourceUri = source.toURI();
		URI targetUri = target.toURI();
		if (sourceUri.getAuthority().equals(targetUri.getAuthority())) {
			return super.move(sourceUri, targetUri, params);
		}

		IOperationHandler nextHandler = manager.findNextHandler(Operation.move(source.getScheme(), target.getScheme()), this);
		if (nextHandler != null) {
			return nextHandler.move(source, target, params);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Overridden to speed up directory listing in File URL dialog.
	 */
	@Override
	protected List<Info> listDirectory(URI uri) throws IOException {
		return s3handler.listFiles(uri);
	}

	/*
	 * Overridden for better performance when deleting recursively
	 */
	@Override
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
				return s3handler.removeDirRecursively(target);
			} else {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.cannot_remove_directory"), target)); //$NON-NLS-1$
			}
		}
		return info.isDirectory() ? simpleHandler.removeDir(target) : simpleHandler.deleteFile(target);
	}

	@Override
	public SingleCloverURI create(SingleCloverURI target, CreateParameters params) throws IOException {
		if (params.getLastModified() != null) {
			throw new UnsupportedOperationException("Setting last modification date is not supported by S3");
		}
		return super.create(target, params);
	}

	@Override
	public String toString() {
		return "S3OperationHandler";
	}

}
