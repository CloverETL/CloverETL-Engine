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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpStatus;
import org.jetel.component.fileoperation.Info.Type;
import org.jetel.component.fileoperation.pool.Authority;
import org.jetel.component.fileoperation.pool.ConnectionPool;
import org.jetel.component.fileoperation.pool.PooledS3Connection;
import org.jetel.component.fileoperation.pool.S3Authority;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.stream.DelegatingOutputStream;
import org.jetel.util.stream.InterruptibleInputStream;
import org.jetel.util.string.StringUtils;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.model.MultipleDeleteResult;
import org.jets3t.service.model.MultipleDeleteResult.ErrorResult;
import org.jets3t.service.model.container.ObjectKeyAndVersion;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.utils.Mimetypes;
import org.jets3t.service.utils.MultipartUtils;
import org.jets3t.service.utils.ServiceUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17. 3. 2015
 */
public class PrimitiveS3OperationHandler implements PrimitiveOperationHandler {
	
	private static final String FORWARD_SLASH = "/";
	
	private static final Log log = LogFactory.getLog(PrimitiveS3OperationHandler.class);

	private ConnectionPool pool = ConnectionPool.getInstance();
	
	/**
	 * Extracts bucket name and key from the URI.
	 * If the key is empty, the returned array has just one element.
	 * For an URI pointing to S3 root,
	 * an array containing one empty string is returned.
	 * 
	 * @param uri
	 * @return [bucketName, key] or [bucketName]
	 */
	protected static String[] getPath(URI uri) {
		String path = uri.getPath();
		if (path.startsWith(FORWARD_SLASH)) {
			path = path.substring(1);
		}
		String[] parts = path.split(FORWARD_SLASH, 2);
		if ((parts.length == 2) && parts[1].isEmpty()) {
			return new String[] {parts[0]};
		}
		return parts;
	}
	
	private static String appendSlash(String input) {
		if (!input.endsWith(FORWARD_SLASH)) {
			input += FORWARD_SLASH;
		}
		return input;
	}

	/**
	 * Creates a regular file.
	 * Fails if the parent directory does not exist.
	 */
	@Override
	public boolean createFile(URI target) throws IOException {
		target = target.normalize();
		PooledS3Connection connection = null;
		try {
			connection = connect(target);
			S3Service service = connection.getService();
			URI parentUri = URIUtils.getParentURI(target);
			if (parentUri != null) {
				Info parentInfo = info(parentUri, connection);
				if (parentInfo == null) {
					throw new IOException("Parent dir does not exist");
				}
			}
			String[] path = getPath(target);
			if (path.length == 1) {
				throw new IOException("Cannot write to the root directory");
			}
			S3Object file = new S3Object(path[1]);
			try {
				service.putObject(path[0], file);
				return true;
			} catch (S3ServiceException e) {
				throw new IOException(e);
			}
		} finally {
			disconnect(connection);
		}
	}

	@Override
	public boolean setLastModified(URI target, Date date) throws IOException {
		// not supported by S3
		return false;
	}

	/**
	 * Creates a directory.
	 * Fails if the parent directory does not exist.
	 */
	@Override
	public boolean makeDir(URI target) throws IOException {
		PooledS3Connection connection = null;
		try {
			connection = connect(target);
			S3Service service = connection.getService();
			URI parentUri = URIUtils.getParentURI(target);
			if (parentUri != null) {
				Info parentInfo = info(parentUri, connection);
				if (parentInfo == null) {
					throw new IOException("Parent dir does not exist");
				}
			}
			String[] path = getPath(target);
			String bucketName = path[0];
			try {
				if (path.length == 1) {
					service.createBucket(bucketName);
				} else {
					String dirName = appendSlash(path[1]);
					S3Object dir = new S3Object(dirName);
					service.putObject(bucketName, dir);
				}
				return true;
			} catch (S3ServiceException e) {
				throw new IOException(e);
			}
		} finally {
			disconnect(connection);
		}
	}

	/**
	 * Deletes a regular file.
	 */
	@Override
	public boolean deleteFile(URI target) throws IOException {
		target = target.normalize();
		PooledS3Connection connection = null;
		try {
			connection = connect(target);
			S3Service service = connection.getService();
			String[] path = getPath(target);
			try {
				service.deleteObject(path[0], path[1]);
				return true;
			} catch (ServiceException e) {
				throw new IOException(e);
			}
		} finally {
			disconnect(connection);
		}
	}

	/**
	 * Removes a directory.
	 */
	@Override
	public boolean removeDir(URI target) throws IOException {
		target = target.normalize();
		PooledS3Connection connection = null;
		try {
			connection = connect(target);
			S3Service service = connection.getService();
			String[] path = getPath(target);
			String bucketName = path[0];
			try {
				if (path.length == 1) {
					if (bucketName.isEmpty()) {
						throw new IOException("Unable to delete root directory");
					}
					service.deleteBucket(bucketName);
				} else {
					String dirName = appendSlash(path[1]);
					service.deleteObject(bucketName, dirName);
				}
				return true;
			} catch (ServiceException e) {
				throw new IOException(e);
			}
		} finally {
			disconnect(connection);
		}
	}

	/**
	 * CLO-6159:
	 * 
	 * @param target
	 * @return
	 */
	public boolean removeDirRecursively(URI target) throws IOException {
		target = target.normalize();
		PooledS3Connection connection = null;
		try {
			connection = connect(target);
			S3Service service = connection.getService();
			String[] path = getPath(target);
			String bucketName = path[0];
			try {
				if (path.length == 1) {
					if (bucketName.isEmpty()) {
						throw new IOException("Unable to delete root directory");
					}
					S3Object[] objects = service.listObjects(bucketName);
					deleteObjects(service, bucketName, objects);
					service.deleteBucket(bucketName);
				} else {
					String dirName = appendSlash(path[1]);
					S3Object[] objects = service.listObjects(bucketName, dirName, null); // no delimiter!
					deleteObjects(service, bucketName, objects);
				}
				return true;
			} catch (ServiceException e) {
				throw new IOException(e);
			}
		} finally {
			disconnect(connection);
		}
	}

	private void deleteObjects(S3Service service, String bucketName, S3Object[] objects) throws S3ServiceException {
		if (objects.length > 0) {
			ObjectKeyAndVersion[] keys = new ObjectKeyAndVersion[objects.length];
			for (int i = 0; i < objects.length; i++) {
				keys[i] = new ObjectKeyAndVersion(objects[i].getKey());
			}
			MultipleDeleteResult result = service.deleteMultipleObjects(bucketName, keys, true); // quiet
			if (result.hasErrors()) {
				ErrorResult err = result.getErrorResults().get(0);
				// TODO add remaining errors as suppressed exceptions
				S3ServiceException ex = new S3ServiceException("Failed to delete " + err.getKey());
				ex.setErrorMessage(err.getMessage());
				ex.setErrorCode(ex.getErrorCode());
				throw ex;
			}
		}
	}

	@Override
	public URI moveFile(URI source, URI target) throws IOException {
		return null;
	}

	protected static void putObject(S3Service service, File file, String targetBucket, String targetKey) throws IOException {
		S3Object targetObject = createS3Object(file, targetKey);
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
		}
		try {
			service.putObjectMaybeAsMultipart(targetBucket, targetObject, MultipartUtils.MAX_OBJECT_SIZE);
		} catch (ServiceException e) {
			throw getIOException(e);
		}
	}

	/*
	 * Wraps all used InputStreams in InterruptibleInputStream
	 * to make uploads interruptible.
	 * ------------------------------------------------------------------------
	 * Based on org.jets3t.service.model.S3Object.S3Object(File):
	 * 
	 * JetS3t : Java S3 Toolkit
	 * Project hosted at http://bitbucket.org/jmurty/jets3t/
	 *
	 * Copyright 2006-2010 James Murty
	 *
	 * Licensed under the Apache License, Version 2.0 (the "License");
	 * you may not use this file except in compliance with the License.
	 * You may obtain a copy of the License at
	 *
	 *     http://www.apache.org/licenses/LICENSE-2.0
	 *
	 * Unless required by applicable law or agreed to in writing, software
	 * distributed under the License is distributed on an "AS IS" BASIS,
	 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	 * See the License for the specific language governing permissions and
	 * limitations under the License.
	 */
    private static S3Object createS3Object(File file, String key) throws IOException {
		try {
			S3Object o = new S3Object(key) {

				@Override
				public InputStream getDataInputStream() throws ServiceException {
					return new InterruptibleInputStream(super.getDataInputStream());
				}
				
			};
	        o.setContentLength(file.length());
	        o.setContentType(Mimetypes.getInstance().getMimetype(file));
	        if (!file.exists()) {
	            throw new FileNotFoundException("Cannot read from file: " + file.getAbsolutePath());
	        }
	        o.setDataInputFile(file);
	        o.setMd5Hash(ServiceUtils.computeMD5Hash(new InterruptibleInputStream(new FileInputStream(file))));
	        o.setSHA256Hash(ServiceUtils.hashSHA256(new InterruptibleInputStream(new FileInputStream(file))));
			return o;
		} catch (Exception ex) {
			throw getIOException(ex);
		}
	}
	
	/**
	 * Performs server-side copy of a regular file.
	 */
	@Override
	public URI copyFile(URI source, URI target) throws IOException {
		source = source.normalize();
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
			String[] sourcePath = getPath(source);
			if (sourcePath.length == 1) {
				throw new IOException("Cannot read from " + source);
			}
			String[] targetPath = getPath(target);
			if (targetPath.length == 1) {
				throw new IOException("Cannot write to " + target);
			}
			String sourceBucket = sourcePath[0];
			String sourceKey = sourcePath[1];
			String targetBucket = targetPath[0];
			String targetKey = targetPath[1];
			S3Object targetObject = new S3Object(targetKey);
			try {
				// server-side copy!
				service.copyObject(sourceBucket, sourceKey, targetBucket, targetObject, false);
				return target;
			} catch (ServiceException e) {
				throw new IOException(e);
			}
		} finally {
			disconnect(connection);
		}
	}

	@Override
	public URI renameTo(URI source, URI target) throws IOException {
		// not supported, S3Service.moveObject() just performs copy and delete anyway
		// would only work for files, not for directories
		return null;
	}

	@Override
	public ReadableByteChannel read(URI source) throws IOException {
		PooledS3Connection connection = connect(source);
		return Channels.newChannel(getInputStream(source, connection));
	}

	@Override
	public WritableByteChannel write(URI target) throws IOException {
		PooledS3Connection connection = connect(target);
		return Channels.newChannel(getOutputStream(target, connection));
	}

	@Override
	public WritableByteChannel append(URI target) throws IOException {
		throw new UnsupportedOperationException("Appending is not supported by S3");
	}
	
	private static Info getBucketInfo(String bucketName, URI baseUri) throws IOException {
		SimpleInfo info = new SimpleInfo(bucketName, getUri(baseUri, bucketName));
		info.setType(Type.DIR);
		return info;
	}
	
	private static Info getDirectoryInfo(String dirName, URI uri) {
		if (dirName.endsWith(FORWARD_SLASH)) {
			dirName = dirName.substring(0, dirName.length() - 1);
		}
		return new SimpleInfo(dirName, uri).setType(Type.DIR);
	}
	
	/**
	 * Returns an {@link Info} instance for the specified bucketName and key.
	 * If the key does not end with a slash,
	 * tries to search first for a file and then for a directory.
	 * A directory file may not physically exist,
	 * it may exist just as a commons prefix of some object keys.
	 * 
	 * @param connection
	 * @param bucketName
	 * @param key
	 * @return
	 * @throws IOException
	 */
	private static Info getFileOrDirectory(PooledS3Connection connection, String bucketName, String key) throws IOException {
		S3Service service = connection.getService();
		try {
			// avoid using LIST, it is slower and more expensive
			// use getObjectDetails() instead of getObject() to avoid downloading the content
			StorageObject object = service.getObjectDetails(bucketName, key);
			return new S3ObjectInfo(object, connection.getBaseUri());
		} catch (ServiceException e) {
			if ((e.getResponseCode() == HttpStatus.SC_NOT_FOUND)
	                || "NoSuchKey".equals(e.getErrorCode())) {
				if (key.endsWith(FORWARD_SLASH)) { // try to find a "virtual" directory
					try {
						// directory object may not physically exist, but there may be a matching prefix
						StorageObjectsChunk chunk = service.listObjectsChunked(bucketName, key.substring(0, key.length() - 1), FORWARD_SLASH, 0, null);
						String[] directories = chunk.getCommonPrefixes();
						for (String dir: directories) {
							if (dir.equals(key)) {
								S3Object object = new S3Object(key);
								object.setBucketName(bucketName); // important!
								return new S3ObjectInfo(object, connection.getBaseUri());
							}
						}
					} catch (ServiceException listingException) {
						if (listingException.getResponseCode() == HttpStatus.SC_NOT_FOUND) { // listObjectsChunked() may also return 404
							return null;
						}
						throw new IOException(listingException);
					}
				}
				return null;
			} else if ("NoSuchBucket".equals(e.getErrorCode())) {
				return null;
			} else {
				throw new IOException(e);
			}
		}
	}

	protected static Info info(URI target, PooledS3Connection connection) throws IOException {
		String[] path = getPath(target);
		return info(path, connection);
	}

	private static Info info(String[] path, PooledS3Connection connection) throws IOException {
		S3Service service = connection.getService();
		String bucketName = path[0];
		if (path.length == 1) { // just the bucket
			if (bucketName.isEmpty()) {
				// TODO test connection
				return new SimpleInfo("", connection.getBaseUri()).setType(Type.DIR); // root
			}
			try {
				S3Bucket bucket = service.getBucket(bucketName);
				if (bucket != null) {
					return getBucketInfo(bucketName, connection.getBaseUri());
				} else {
					return null;
				}
			} catch (ServiceException e) {
				throw new IOException(e);
			}
		} else {
			String key = path[1];
			Info info = getFileOrDirectory(connection, bucketName, key);
			if ((info == null) && !key.endsWith(FORWARD_SLASH)) {
				// check for a directory with the same name
				info = getFileOrDirectory(connection, bucketName, key + FORWARD_SLASH);
			}
			return info;
		}
	}

	@Override
	public Info info(URI target) throws IOException {
		target = target.normalize();
		PooledS3Connection connection = null;
		try {
			connection = connect(target);
			return info(target, connection);
		} finally {
			disconnect(connection);
		}
	}
	
	public List<Info> listFiles(URI target) throws IOException {
		target = target.normalize();
		PooledS3Connection connection = null;
		try {
			connection = connect(target);
			S3Service service = connection.getService();
			String[] path = getPath(target);
			String bucketName = path[0];
			try {
				URI baseUri = connection.getBaseUri();
				List<Info> result;
				if (bucketName.isEmpty()) { // root - list buckets
					S3Bucket[] buckets = service.listAllBuckets();
					result = new ArrayList<Info>(buckets.length);
					for (S3Bucket bucket: buckets) {
						result.add(getBucketInfo(bucket.getName(), baseUri));
					}
				} else {
					String prefix = "";
					if (path.length > 1) {
						prefix = appendSlash(path[1]);
					}
					StorageObjectsChunk chunk = service.listObjectsChunked(bucketName, prefix, FORWARD_SLASH, Integer.MAX_VALUE, null, true);
					String[] directories = chunk.getCommonPrefixes();
					StorageObject[] files = chunk.getObjects();
					result = new ArrayList<Info>(directories.length + files.length);
					int prefixLength = prefix.length();
					
					for (String directory: directories) {
						String name = directory.substring(prefixLength);
						URI uri = URIUtils.getChildURI(target, name);
						result.add(getDirectoryInfo(name, uri));
					}
					
					for (StorageObject object: files) {
						String key = object.getKey();
						if (key.length() > prefixLength) { // skip the parent directory itself
							S3ObjectInfo info = new S3ObjectInfo(object, baseUri);
							result.add(info);
						}
					}
				}
				return result;
			} catch (ServiceException e) {
				throw getIOException(e);
			}
		} finally {
			disconnect(connection);
		}
	}

	@Override
	public List<URI> list(URI target) throws IOException {
		List<Info> files = listFiles(target);
		List<URI> result = new ArrayList<>(files.size());
		for (Info file: files) {
			result.add(file.getURI());
		}
		return result;
	}
	
	/**
	 * Resolves the specified path against baseUri,
	 * escaping special characters in the path.
	 * 
	 * @param baseUri
	 * @param path
	 * @return
	 * @throws IOException
	 */
	private static URI getUri(URI baseUri, String path) throws IOException {
		try {
			// escape special characters in the path
			URI pathUri = new URI(null, null, path, null);
			return baseUri.resolve(pathUri);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}
	
	private static class S3ObjectInfo implements Info {
		
		private final StorageObject object;
		
		private final URI uri;
		
		private final boolean directory;
		
		public S3ObjectInfo(StorageObject object, URI baseUri) throws IOException {
			this.object = object;
			if (baseUri != null) {
				StringBuilder path = new StringBuilder();
				path.append(object.getBucketName());
				path.append('/');
				path.append(object.getKey());
				this.uri = getUri(baseUri, path.toString());
			} else {
				this.uri = null;
			}
			this.directory = object.getKey().endsWith(FORWARD_SLASH);
		}

		@Override
		public String getName() {
			String key = object.getKey();
			int length = key.length();
			if (directory) {
				length--; // ignore the trailing slash
			}
	        int index = key.lastIndexOf('/', length - 1);
	        return key.substring(index + 1, length);
		}

		@Override
		public URI getURI() {
			return uri;
		}

		@Override
		public URI getParentDir() {
			// TODO not implemented, does not seem to be necessary
			return null;
		}

		@Override
		public boolean isDirectory() {
			return directory;
		}

		@Override
		public boolean isFile() {
			return !directory;
		}

		@Override
		public Boolean isLink() {
			return false;
		}

		@Override
		public Boolean isHidden() {
			return false;
		}

		@Override
		public Boolean canRead() {
			// TODO read from ACL and policy? how about inheritance?
			return null;
		}

		@Override
		public Boolean canWrite() {
			// TODO read from ACL?
			return null;
		}

		@Override
		public Boolean canExecute() {
			// TODO read from ACL?
			return null;
		}

		@Override
		public Type getType() {
			return directory ? Type.DIR : Type.FILE;
		}

		@Override
		public Date getLastModified() {
			return directory ? null : object.getLastModifiedDate();
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
			return directory ? null : object.getContentLength();
		}

		@Override
		public String toString() {
			return (uri != null) ? uri.toString() : super.toString();
		}
		
	}
	
	public static InputStream getInputStream(URI uri, final PooledS3Connection connection) throws IOException {
		try {
			uri = uri.normalize();
			S3Service service = connection.getService();
			String[] path = getPath(uri);
			String bucketName = path[0];
			if (path.length < 2) {
				throw new IOException(StringUtils.isEmpty(bucketName) ? "Cannot read from the root directory" : "Cannot read from bucket root directory");
			}
			S3Object object = service.getObject(bucketName, path[1]);
			InputStream is = object.getDataInputStream();
			if (is == null) {
				throw new IOException("No data available");
			}
			is = new FilterInputStream(is) {
				@Override
				public void close() throws IOException {
					try {
						super.close();
					} finally {
						connection.returnToPool();
					}
				}
			};
			return is;
		} catch (Exception e) {
			connection.returnToPool();
			throw getIOException(e);
		}
	}

	/**
	 * Returns an {@link OutputStream} instance that writes data
	 * to a temp file, uploads it to S3 when the stream is closed
	 * and deletes the temp file.
	 * 
	 * If the file size exceeds 5 GB, performs multipart upload.
	 * 
	 * @param bucketName
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public static OutputStream getOutputStream(URI uri, final PooledS3Connection connection) throws IOException {
		try {
			uri = uri.normalize();
			final S3Service service = connection.getService();
			String[] path = getPath(uri);
			final String bucketName = path[0];
			if (path.length < 2) {
				throw new IOException(StringUtils.isEmpty(bucketName) ? "Cannot write to the root directory" : "Cannot write to bucket root directory");
			}
			final String key = path[1];
			
			URI parentUri = URIUtils.getParentURI(uri);
			if (parentUri != null) {
				Info parentInfo = info(parentUri, connection);
				if (parentInfo == null) {
					throw new IOException("Parent dir does not exist");
				}
			}
			
			Info info = info(path, connection);
			if ((info != null) && info.isDirectory()) {
				throw new IOException(MessageFormat.format(FileOperationMessages.getString("IOperationHandler.exists_not_file"), uri)); //$NON-NLS-1$
			}
			
			final File tempFile = IAuthorityProxy.getAuthorityProxy(ContextProvider.getGraph()).newTempFile("cloveretl-amazons3-buffer", -1);
			
			OutputStream os = new DelegatingOutputStream(new FileOutputStream(tempFile)) {
				
				private final AtomicBoolean uploaded = new AtomicBoolean(false);

				@Override
				public void close() throws IOException {
					upload(); // closes and uploads the file
				}

				@Override
				protected void finalize() throws Throwable {
					try {
						super.finalize();
					} finally {
						upload();
					}
				}
				
				private void upload() throws IOException {
					try {
						super.close(); // the file must be closed before upload
					} finally {
						if (uploaded.compareAndSet(false, true)) {
							try {
								// CLO-4724:
								putObject(service, tempFile, bucketName, key);
							} finally {
								connection.returnToPool();
								if (!tempFile.delete()) {
									log.warn("Failed to delete temp file: " + tempFile);
								}
							}
						}
					}
				}
				
			};
			
			return os;
		} catch (Exception e) {
			connection.returnToPool();
			throw getIOException(e);
		}
	}

	protected PooledS3Connection connect(URI uri) throws IOException {
		try {
			Authority authority = new S3Authority(uri);
			return (PooledS3Connection) pool.borrowObject(authority);
		} catch (IOException ioe) {
			throw ioe;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	protected void disconnect(PooledS3Connection connection) {
		if (connection != null) {
			try {
				pool.returnObject(connection.getAuthority(), connection);
			} catch (Exception ex) {
				log.debug("Failed to return S3 connection to the pool", ex);
			}
		}
	}

	@Override
	public String toString() {
		return "PrimitiveS3OperationHandler";
	}
	
	private static IOException s3ExceptionToIOException(S3ServiceException e) {
		StringBuilder msg = new StringBuilder();
		msg.append("S3 Service Error.");
		appendInfoIfNotNull("Response code=" + e.getResponseCode(), msg);
		appendInfoIfNotNull(e.getResponseStatus(), msg);
		appendInfoIfNotNull(e.getS3ErrorCode(), msg);
		appendInfoIfNotNull(e.getS3ErrorMessage(), msg);
		appendInfoIfNotNull(e.toString(), msg);
		msg.append('\n');
		return new IOException(msg.toString(), e);
	}
	
	private static IOException serviceExceptionToIOException(ServiceException e) {
		StringBuilder msg = new StringBuilder();
		msg.append("S3 Service Error.");
		appendInfoIfNotNull("Response code=" + e.getResponseCode(), msg);
		appendInfoIfNotNull(e.getResponseStatus(), msg);
		appendInfoIfNotNull(e.getErrorCode(), msg);
		appendInfoIfNotNull(e.getErrorMessage(), msg);
		msg.append('\n');
		appendInfoIfNotNull(e.toString(), msg);
		return new IOException(msg.toString(), e);
	}
	
	private static void appendInfoIfNotNull(String info, StringBuilder text) {
		if (info != null && !info.isEmpty()) {
			if (text.length() > 0) {
				text.append(" ");
			}
			text.append(info);
			if (!info.endsWith(".")) {
				text.append(".");
			}
		}
	}

	public static IOException getIOException(Throwable t) {
		if (t instanceof S3ServiceException) {
			return s3ExceptionToIOException((S3ServiceException) t);
		} else if (t instanceof ServiceException) {
			return serviceExceptionToIOException((ServiceException) t);
		}
		return ExceptionUtils.getIOException(t);
	}

}
