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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.text.MessageFormat;
import java.util.List;

import org.jetel.component.fileoperation.SimpleParameters.CopyParameters;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.SimpleParameters.MoveParameters;
import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;
import org.jetel.component.fileoperation.result.CopyResult;
import org.jetel.component.fileoperation.result.DeleteResult;
import org.jetel.component.fileoperation.result.InfoResult;
import org.jetel.component.fileoperation.result.ListResult;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.stream.StreamUtils;

public class DefaultOperationHandler extends BaseOperationHandler {
	
	protected FileManager manager = FileManager.getInstance();
	
	@Override
	public int getPriority(Operation operation) {
		return Integer.MIN_VALUE;
	}

	@Override
	public boolean canPerform(Operation operation) {
		switch (operation.kind) {
			case COPY:
				return manager.canPerform(Operation.resolve(operation.scheme(0)))
						&& manager.canPerform(Operation.info(operation.scheme(0)))
						&& manager.canPerform(Operation.info(operation.scheme(1)))
						&& manager.canPerform(Operation.read(operation.scheme(0)))
						&& manager.canPerform(Operation.list(operation.scheme(0)))
						&& manager.canPerform(Operation.create(operation.scheme(1)))
						&& manager.canPerform(Operation.write(operation.scheme(1)));
			case MOVE:
				return manager.canPerform(Operation.copy(operation.scheme(0), operation.scheme(1)))
						&& manager.canPerform(Operation.delete(operation.scheme(0)));
			default: 
				return false;
		}
	}
	
	private boolean copyFile(SingleCloverURI source, SingleCloverURI target, Long sourceSize) throws IOException {
		try (
			ReadableByteChannel inputChannel = manager.getInput(source).channel();
			WritableByteChannel outputChannel = manager.getOutput(target).channel();
		) {
			if (sourceSize != null) {
				StreamUtils.copy(inputChannel, outputChannel, sourceSize);
			} else {
				// fallback to handle cases when source size is unknown
				StreamUtils.copy(inputChannel, outputChannel);
			}
			return true;
		}
	}
	
	private final CreateParameters CREATE_DIRECTORY = new CreateParameters().setDirectory(true);
	
	private boolean copyInternal(SingleCloverURI source, SingleCloverURI target, CopyParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		InfoResult sourceInfo = manager.info(source);
		if (!sourceInfo.success()) {
			throw ExceptionUtils.getIOException(sourceInfo.getFirstError());
		}
		InfoResult targetInfo = manager.info(target);
		if (!targetInfo.success()) {
			throw ExceptionUtils.getIOException(targetInfo.getFirstError());
		}
		if (targetInfo.exists()) {
			URI sourceUri = sourceInfo.getURI();
			URI targetUri = targetInfo.getURI();
			if (sourceUri.normalize().equals(targetUri.normalize())) {
				throw new SameFileException(sourceUri, targetUri);
			}
		}
		boolean makeParents = Boolean.TRUE.equals(params.isMakeParents());
		if (sourceInfo.isDirectory()) {
			if (!params.isRecursive()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.cannot_copy_directory"), source)); //$NON-NLS-1$
			}
			if (targetInfo.exists() && !targetInfo.isDirectory()) {
				throw new IOException(format(FileOperationMessages.getString("IOperationHandler.cannot_overwrite_not_a_directory"), source, target)); //$NON-NLS-1$
			}
			if (!targetInfo.exists()) {
				if (!manager.create(target, CREATE_DIRECTORY.clone().setMakeParents(makeParents)).success()) {
					throw new IOException(format(FileOperationMessages.getString("IOperationHandler.create_failed"), target)); //$NON-NLS-1$
				}
				targetInfo = manager.info(target);
			}
			boolean success = true;
			URI parentURI = targetInfo.getURI();
			for (Info child: manager.list(source)) {
				SingleCloverURI childUri = CloverURI.createSingleURI(URIUtils.getChildURI(parentURI, child.getName())).getAbsoluteURI(); 
				success &= copyInternal(CloverURI.createSingleURI(child.getURI()), childUri, params);
			}
			return success;
		} else {
			if (targetInfo.exists()) {
				if (params.isNoOverwrite()) {
					return true;
				}
				if (params.isUpdate()) {
					if (sourceInfo.getLastModified() == null) {
						throw new IOException("Failed to obtain source modification date: " + source);
					}
					if (targetInfo.getLastModified() == null) {
						throw new IOException("Failed to obtain target modification date: " + target);
					}
					if (sourceInfo.getLastModified().compareTo(targetInfo.getLastModified()) <= 0) {
						return true; // CLO-11678 - older file: mark as success; newer file: overwrite it (continue to copyFile() below)
					}
				}
			} else if (makeParents) {
				URI parentUri = URIUtils.getParentURI(target.toURI());
				if (parentUri != null) {
					manager.create(CloverURI.createSingleURI(parentUri), CREATE_DIRECTORY.clone().setMakeParents(true));
				}
			}
			return copyFile(source, target, sourceInfo.getSize());
		}
	}
	
	private void checkSubdir(URI source, URI target) throws IOException {
		String sourcePath = source.normalize().toString();
		String targetPath = target.normalize().toString();
		if (!sourcePath.endsWith(URIUtils.PATH_SEPARATOR)) {
			sourcePath = sourcePath + URIUtils.PATH_SEPARATOR;
		}
		if (targetPath.startsWith(sourcePath)) {
			throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.subdirectory"), target, source)); //$NON-NLS-1$
		}
	}
	
	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target, CopyParameters params) throws IOException {
		InfoResult sourceInfo = manager.info(source);
		if (!sourceInfo.success()) {
			throw new IOException("Failed to obtain source file info", sourceInfo.getFirstError());
		}
		if (!sourceInfo.exists()) {
			// source does not exist
			throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), source.getPath())); //$NON-NLS-1$
		}
		InfoResult targetInfo = manager.info(target);
		if (!targetInfo.success()) {
			throw new IOException("Failed to obtain target file info", targetInfo.getFirstError());
		}
		if (targetInfo.isDirectory()) {
			target = CloverURI.createSingleURI(URIUtils.getChildURI(targetInfo.getURI(), sourceInfo.getName())).getAbsoluteURI();
		} else if (target.getPath().endsWith(URIUtils.PATH_SEPARATOR)) {
			if (Boolean.TRUE.equals(params.isMakeParents())) {
				target = CloverURI.createSingleURI(URIUtils.getChildURI(target.toURI(), sourceInfo.getName())).getAbsoluteURI();
			} else if (!sourceInfo.isDirectory()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.not_a_directory"), target.getPath())); //$NON-NLS-1$
			}
		}
		if (sourceInfo.isDirectory()) {
			checkSubdir(source.toURI(), target.toURI());
		}
		return copyInternal(source, target, params) ? target : null;
	}
	
	private final CopyParameters COPY_RECURSIVE = new CopyParameters().setRecursive(true);
	private final DeleteParameters DELETE_RECURSIVE = new DeleteParameters().setRecursive(true);
	
	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params) throws IOException {
		InfoResult sourceInfo = null;
		InfoResult targetInfo = manager.info(target);
		if (targetInfo.exists()) {
			sourceInfo = manager.info(source);
			URI sourceUri = sourceInfo.exists() ? sourceInfo.getURI() : source.toURI();
			URI targetUri = targetInfo.getURI();
			if (sourceUri.normalize().equals(targetUri.normalize())) {
				throw new SameFileException(sourceUri, targetUri);
			}
		}
		if (sourceInfo == null) {
			sourceInfo = manager.info(source);
		}
		if (targetInfo.isDirectory()) {
			if (!sourceInfo.exists()) {
				throw new FileNotFoundException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.file_not_found"), source.toURI().toString())); //$NON-NLS-1$
			}
			SingleCloverURI newTarget = CloverURI.createSingleURI(URIUtils.getChildURI(target.toURI(), sourceInfo.getName()));
			targetInfo = manager.info(newTarget);
			if (targetInfo.isDirectory()) {
				ListResult list = manager.list(newTarget);
				if (!list.success()) {
					throw new IOException(format(FileOperationMessages.getString("FileManager.failed_to_list_contents"), newTarget.toURI().toString())); //$NON-NLS-1$
				}
				if (!list.isEmpty()) {
					throw new IOException(format(FileOperationMessages.getString("IOperationHandler.not_empty"), newTarget.toURI().toString())); //$NON-NLS-1$
				}
			}
		}
		
		if (sourceInfo.isDirectory()) {
			checkSubdir(source.toURI(), target.toURI());
		}
		
		CopyResult copyResult = manager.copy(source, target, COPY_RECURSIVE.clone().setOverwriteMode(params.getOverwriteMode()).setMakeParents(params.isMakeParents()));
		if (!copyResult.success()) {
			throw new IOException("Copy failed", copyResult.getFirstError());
		}
		DeleteResult deleteResult = manager.delete(source, DELETE_RECURSIVE);
		if (!deleteResult.success()) {
			throw new IOException("Delete failed", deleteResult.getFirstError());
		}
		return copyResult.getResult(0);
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI wildcards, ResolveParameters params) throws IOException {
		return manager.defaultResolve(wildcards);
	}

	@Override
	public String toString() {
		return "DefaultOperationHandler"; //$NON-NLS-1$
	}

}
