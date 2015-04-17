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
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.jetel.component.fileoperation.pool.PooledS3Connection;
import org.jetel.component.fileoperation.result.DeleteResult;
import org.jetel.component.fileoperation.result.InfoResult;
import org.jetel.component.fileoperation.result.ListResult;
import org.jetel.util.stream.StreamUtils;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17. 3. 2015
 */
public class PrimitiveS3CopyOperationHandler extends PrimitiveS3OperationHandler {
	
	private FileManager manager = FileManager.getInstance();

	@Override
	public URI copyFile(URI source, URI target) throws IOException {
		target = target.normalize();
		PooledS3Connection connection = null;
		try {
			connection = connect(target);
			
			URI parentUri = URIUtils.getParentURI(target);
			if (parentUri != null) {
				Info parentInfo = info(parentUri, connection);
				if (parentInfo == null) {
					throw new IOException("Parent directory does not exist");
				}
			}

			S3Service service = connection.getService();

			CloverURI sourceUri = CloverURI.createSingleURI(source);
			File file = null;
			Exception ex1 = null;
			try {
				file = manager.getFile(sourceUri);
			} catch (Exception ex) {
				ex1 = ex;
			}
			if (file != null) {
				return copyLocalFile(file, target, service);
			} else {
				// conversion to File failed (remote sandbox?)
				// perform regular stream copy instead
				try {
					try (ReadableByteChannel inputChannel = manager.getInput(sourceUri).channel()) {
						// getOutputStream() takes ownership of the connection 
						PooledS3Connection outputStreamConnection = connection;
						connection = null; // do not disconnect
						try (
							OutputStream output = getOutputStream(target, outputStreamConnection);
							WritableByteChannel outputChannel = Channels.newChannel(output);
						) {
							StreamUtils.copy(inputChannel, outputChannel);
							return target;
						}
					}
				} catch (Exception ex2) {
					IOException ioe = new IOException("S3 upload failed", ex2);
					if (ex1 != null) {
						ioe.addSuppressed(ex1);
					}
					throw ioe;
				}
			}
		} finally {
			disconnect(connection);
		}
	}
	
	private URI copyLocalFile(File source, URI target, S3Service service) throws IOException {
		String[] targetPath = getPath(target);
		if (targetPath.length == 1) {
			throw new IOException("Cannot write to " + target);
		}
		String targetBucket = targetPath[0];
		String targetKey = targetPath[1];
		S3Object targetObject;
		try {
			targetObject = new S3Object(source);
		} catch (NoSuchAlgorithmException e) {
			throw new IOException(e);
		}
		targetObject.setKey(targetKey);
		try {
			putObject(service, targetBucket, targetObject);
			return target;
		} catch (ServiceException e) {
			throw new IOException(e);
		}
	}

	@Override
	public Info info(URI target) throws IOException {
		if (!target.getScheme().equals(S3OperationHandler.S3_SCHEME)) {
			InfoResult infoResult = manager.info(CloverURI.createSingleURI(target));
			if (infoResult.success()) {
				return infoResult.getInfo();
			} else {
				throw new IOException(infoResult.getFirstError());
			}
		}
		
		return super.info(target);
	}

	@Override
	public boolean deleteFile(URI target) throws IOException {
		if (!target.getScheme().equals(S3OperationHandler.S3_SCHEME)) {
			DeleteResult deleteResult = manager.delete(CloverURI.createSingleURI(target));
			if (deleteResult.success()) {
				return true;
			} else {
				throw new IOException(deleteResult.getFirstError());
			}
		}
		
		return super.deleteFile(target);
	}

	@Override
	public List<Info> listFiles(URI target) throws IOException {
		if (!target.getScheme().equals(S3OperationHandler.S3_SCHEME)) {
			ListResult listResult = manager.list(CloverURI.createSingleURI(target));
			if (listResult.success()) {
				return listResult.getResult();
			} else {
				throw new IOException(listResult.getFirstError());
			}
		}

		return super.listFiles(target);
	}


}
