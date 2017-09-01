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
package org.jetel.hadoop.fileoperation;

import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.util.Date;
import java.util.List;

import org.jetel.component.fileoperation.AbstractOperationHandler;
import org.jetel.component.fileoperation.FileManager;
import org.jetel.component.fileoperation.FileOperationMessages;
import org.jetel.component.fileoperation.IOperationHandler;
import org.jetel.component.fileoperation.Info;
import org.jetel.component.fileoperation.Operation;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.MoveParameters;
import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;
import org.jetel.component.fileoperation.SingleCloverURI;
import org.jetel.hadoop.connection.HadoopURLUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Nov 26, 2012
 */
public class HadoopOperationHandler extends AbstractOperationHandler {

	public static final String HADOOP_SCHEME = HadoopURLUtils.HDFS_PROTOCOL;

	private FileManager manager = FileManager.getInstance();
	
	private PrimitiveHadoopOperationHandler hadoopHandler;

	public HadoopOperationHandler() {
		super(new PrimitiveHadoopOperationHandler());
		this.hadoopHandler = (PrimitiveHadoopOperationHandler) simpleHandler;
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI wildcards, ResolveParameters params) throws IOException {
		return manager.defaultResolve(wildcards);
//		if (!FileManager.hasWildcards(wildcards.getPath())) {
//			return Arrays.asList(wildcards);
//		}
//		// URL-escape []{} characters
//		StringBuilder glob = new StringBuilder(wildcards.getPath());
//		for (int i = 0; i < glob.length(); i++) {
//			char c = glob.charAt(i);
//			switch (c) {
//			case '[':
//			case ']':
//			case '{':
//			case '}':
//				String hex = Integer.toHexString(c);
//				glob.replace(i, i+1, "%" + hex);
//				i += hex.length();
//			}
//		}
//		List<URI> resolved = hadoopHandler.resolve(URI.create(glob.toString()));
//		List<SingleCloverURI> result = new ArrayList<SingleCloverURI>(resolved.size());
//		for (URI uri: resolved) {
//			result.add(CloverURI.createSingleURI(uri));
//		}
//		return result;
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
//			case FILE:
				return operation.scheme().equalsIgnoreCase(HADOOP_SCHEME);
//			case COPY:
			case MOVE:
				return operation.scheme(0).equalsIgnoreCase(HADOOP_SCHEME)
						&& operation.scheme(1).equalsIgnoreCase(HADOOP_SCHEME);
			default: 
				return false;
		}
	}

	@Override
	public String toString() {
		return "HadoopOperationHandler";
	}

	/*
	 * Overridden for better performance when creating parent dirs
	 */
	@Override
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
			if (createDirectory) {
				success = hadoopHandler.makeDir(uri, createParents);
			} else {
				success = hadoopHandler.createFile(uri, createParents);
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

	/*
	 * Overridden to delegate moving between different connections
	 * to DefaultOperationHandler.
	 */
	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target,
			MoveParameters params) throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		if (source.toURI().getAuthority().equals(target.toURI().getAuthority())) {
			// same connection
			return super.move(source, target, params);
		} else {
			// different connections
			IOperationHandler nextHandler = manager.findNextHandler(Operation.move(source.getScheme(), target.getScheme()), this);
			if (nextHandler != null) {
				return nextHandler.move(source, target, params);
			} else {
				throw new UnsupportedOperationException();
			}
		}
	}

}
