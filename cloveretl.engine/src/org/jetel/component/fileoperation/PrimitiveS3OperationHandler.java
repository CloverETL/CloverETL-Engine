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

import static org.jetel.util.protocols.amazon.S3Utils.FORWARD_SLASH;
import static org.jetel.util.protocols.amazon.S3Utils.getPath;
import static org.jetel.util.protocols.amazon.S3Utils.getInputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
import org.jetel.util.protocols.amazon.S3Utils;
import org.jetel.util.stream.DelegatingOutputStream;
import org.jetel.util.string.StringUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager; 

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17. 3. 2015
 */
public class PrimitiveS3OperationHandler implements PrimitiveOperationHandler {
	
	private static final Log log = LogFactory.getLog(PrimitiveS3OperationHandler.class);
	
	private ConnectionPool pool = ConnectionPool.getInstance();
	
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
			AmazonS3 service = connection.getService();
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
			try {
				S3Utils.createEmptyObject(service, path[0], path[1]);
				return true;
			} catch (AmazonClientException e) {
				throw S3Utils.getIOException(e);
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
			AmazonS3 service = connection.getService();
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
					S3Utils.createEmptyObject(service, bucketName, dirName);
				}
				return true;
			} catch (AmazonClientException e) {
				throw S3Utils.getIOException(e);
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
			AmazonS3 service = connection.getService();
			String[] path = getPath(target);
			try {
				service.deleteObject(path[0], path[1]);
				return true;
			} catch (AmazonClientException e) {
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
			AmazonS3 service = connection.getService();
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
			} catch (AmazonClientException e) {
				throw S3Utils.getIOException(e);
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
			AmazonS3 service = connection.getService();
			String[] path = getPath(target);
			String bucketName = path[0];
			try {
				if (path.length == 1) {
					if (bucketName.isEmpty()) {
						throw new IOException("Unable to delete root directory");
					}
					deleteObjects(service, service.listObjects(bucketName));
					service.deleteBucket(bucketName);
				} else {
					String dirName = appendSlash(path[1]);
					deleteObjects(service, service.listObjects(bucketName, dirName)); // no delimiter!
				}
				return true;
			} catch (AmazonClientException e) {
				throw S3Utils.getIOException(e);
			}
		} finally {
			disconnect(connection);
		}
	}

	private void deleteObjects(AmazonS3 service, ObjectListing listing) throws MultiObjectDeleteException, IOException {
		do {
			if (Thread.currentThread().isInterrupted()) {
				throw new IOException(FileOperationMessages.getString("IOperationHandler.interrupted")); //$NON-NLS-1$
			}
			List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
			if (!objectSummaries.isEmpty()) {
				List<KeyVersion> keys = new ArrayList<KeyVersion>(objectSummaries.size());
				for (S3ObjectSummary object: objectSummaries) {
					keys.add(new KeyVersion(object.getKey()));
				}
				DeleteObjectsRequest request = new DeleteObjectsRequest(listing.getBucketName()).withKeys(keys).withQuiet(true);
				service.deleteObjects(request); // quiet
			}
			listing = service.listNextBatchOfObjects(listing);
		} while (listing.isTruncated());
	}

	@Override
	public URI moveFile(URI source, URI target) throws IOException {
		return null;
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
			
			AmazonS3 service = connection.getService();
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
			try {
				// server-side copy!
				service.copyObject(sourceBucket, sourceKey, targetBucket, targetKey);
				return target;
			} catch (AmazonClientException e) {
				throw S3Utils.getIOException(e);
			}
		} finally {
			disconnect(connection);
		}
	}

	@Override
	public URI renameTo(URI source, URI target) throws IOException {
		// not supported, AmazonS3.moveObject() just performs copy and delete anyway
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
		AmazonS3 service = connection.getService();
		try {
			// avoid using LIST, it is slower and more expensive
			// use getObjectMetadata() instead of getObject() to avoid downloading the content
			ObjectMetadata metadata = service.getObjectMetadata(bucketName, key);
			S3ObjectSummary object = new S3ObjectSummary();
			object.setBucketName(bucketName);
			object.setKey(key);
			object.setLastModified(metadata.getLastModified());
			object.setSize(metadata.getInstanceLength());
			return new S3ObjectInfo(object, connection.getBaseUri());
		} catch (AmazonServiceException e) {
			if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
				if (key.endsWith(FORWARD_SLASH)) { // try to find a "virtual" directory
					try {
						// directory object may not physically exist, but there may be a matching prefix
						ListObjectsRequest request = S3Utils.listObjectRequest(bucketName, key.substring(0, key.length() - 1), FORWARD_SLASH);
						ObjectListing listing = service.listObjects(request);
						do {
							List<String> directories = listing.getCommonPrefixes();
							for (String dir: directories) {
								if (dir.equals(key)) {
									S3ObjectSummary object = new S3ObjectSummary();
									object.setKey(key);
									object.setBucketName(bucketName);
									return new S3ObjectInfo(object, connection.getBaseUri());
								}
							}
							listing = service.listNextBatchOfObjects(listing);
						} while (listing.isTruncated());
					} catch (AmazonServiceException listingException) {
						if (listingException.getStatusCode() == HttpStatus.SC_NOT_FOUND) { // listObjectsChunked() may also return 404
							return null;
						}
						throw new IOException(listingException);
					}
				}
				return null;
			}
			throw new IOException(e);
		} catch (Exception ex) {
			throw S3Utils.getIOException(ex);
		}
	}

	protected static Info info(URI target, PooledS3Connection connection) throws IOException {
		String[] path = getPath(target);
		return info(path, connection);
	}

	private static Info info(String[] path, PooledS3Connection connection) throws IOException {
		AmazonS3 service = connection.getService();
		String bucketName = path[0];
		if (path.length == 1) { // just the bucket
			if (bucketName.isEmpty()) {
				// TODO test connection
				return new SimpleInfo("", connection.getBaseUri()).setType(Type.DIR); // root
			}
			try {
				// use doesBucketExist() by default; getBucketLocation() throws an exception for non-existing buckets
	        	if (service.doesBucketExist(bucketName)) {
					return getBucketInfo(bucketName, connection.getBaseUri());
				} else {
					return null;
				}
			} catch (AmazonServiceException e) {
				// CLO-8739
				if (e.getStatusCode() == HttpStatus.SC_BAD_REQUEST) { // only for HTTP 400
			        log.warn("Attempting to re-send the request with AWS V4 authentication. "
			                + "To avoid this warning in the future, please use region-specific endpoint to access "
			                + "buckets located in regions that require V4 signing.");
			        try {
			        	// avoid using listBuckets(), because LIST requests are ten times more expensive
			        	// getBucketLocation() throws an exception for non-existing buckets
			        	String bucketLocation = service.getBucketLocation(bucketName);
			        	if (!StringUtils.isEmpty(bucketLocation)) {
			        		return getBucketInfo(bucketName, connection.getBaseUri());
			        	}
			        } catch (AmazonServiceException ex2) {
			        	if (ex2.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
			        		return null; // HTTP 404
			        	}
			        	ex2.addSuppressed(e);
						throw S3Utils.getIOException(ex2);
			        } catch (AmazonClientException ex2) {
			        	ex2.addSuppressed(e);
						throw S3Utils.getIOException(ex2);
			        }
				}
				throw S3Utils.getIOException(e);
			} catch (AmazonClientException e) {
				throw S3Utils.getIOException(e);
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
			return listFiles(target, connection);
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
		
		private final S3ObjectSummary object;
		
		private final URI uri;
		
		private final boolean directory;
		
		public S3ObjectInfo(S3ObjectSummary object, URI baseUri) throws IOException {
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
			return directory ? null : object.getLastModified();
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
			return directory ? null : object.getSize();
		}

		@Override
		public String toString() {
			return (uri != null) ? uri.toString() : super.toString();
		}
		
	}
	
	public static ObjectMetadata getObjectMetadata(URI uri, AmazonS3 service) throws IOException {
		try {
			String[] path = getPath(uri);
			return service.getObjectMetadata(path[0], path[1]);
		} catch (Exception e) {
			throw S3Utils.getIOException(e);
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
			final TransferManager tm = connection.getTransferManager();
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
								S3Utils.uploadFile(tm, tempFile, bucketName, key);
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
			throw S3Utils.getIOException(e);
		}
	}
	
	public static List<Info> listFiles(URI target, PooledS3Connection connection) throws IOException {
		AmazonS3 service = connection.getService();
		String[] path = getPath(target);
		String bucketName = path[0];
		try {
			URI baseUri = connection.getBaseUri();
			List<Info> result;
			if (bucketName.isEmpty()) { // root - list buckets
				List<Bucket> buckets = service.listBuckets();
				result = new ArrayList<Info>(buckets.size());
				for (Bucket bucket: buckets) {
					result.add(getBucketInfo(bucket.getName(), baseUri));
				}
			} else {
				String prefix = "";
				if (path.length > 1) {
					prefix = appendSlash(path[1]);
				}
				ListObjectsRequest request = S3Utils.listObjectRequest(bucketName, prefix, FORWARD_SLASH);
				ObjectListing listing = service.listObjects(request);
				ArrayList<Info> files = new ArrayList<Info>();
				ArrayList<Info> directories = new ArrayList<Info>();
				int prefixLength = prefix.length();
				
				do {
					for (String directory: listing.getCommonPrefixes()) {
						String name = directory.substring(prefixLength);
						URI uri = URIUtils.getChildURI(target, name);
						directories.add(getDirectoryInfo(name, uri));
					}
					
					for (S3ObjectSummary object: listing.getObjectSummaries()) {
						String key = object.getKey();
						if (key.length() > prefixLength) { // skip the parent directory itself
							S3ObjectInfo info = new S3ObjectInfo(object, baseUri);
							files.add(info);
						}
					}
					
					listing = service.listNextBatchOfObjects(listing);
				} while (listing.isTruncated());
				
				directories.ensureCapacity(directories.size() + files.size());
				result = directories;
				result.addAll(files);
			}
			return result;
		} catch (AmazonClientException e) {
			throw S3Utils.getIOException(e);
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
	
}
