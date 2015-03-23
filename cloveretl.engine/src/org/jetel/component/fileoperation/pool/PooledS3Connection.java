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
package org.jetel.component.fileoperation.pool;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jetel.graph.ContextProvider;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jets3t.service.Constants;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.MultipartUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 3. 2015
 */
public class PooledS3Connection extends AbstractPoolableConnection {

	/*
	 * Jets3tProperties configuration keys
	 */
	private static final String S3SERVICE_S3_ENDPOINT = "s3service.s3-endpoint";
	private static final String S3SERVICE_S3_ENDPOINT_HTTP_PORT = "s3service.s3-endpoint-http-port";
	private static final String S3SERVICE_S3_ENDPOINT_HTTPS_PORT = "s3service.s3-endpoint-https-port";

	/**
	 * Shared instance, do not modify!
	 */
	private static final Jets3tProperties DEFAULTS = Jets3tProperties.getInstance(Constants.JETS3T_PROPERTIES_FILENAME);
	
	/**
	 * Shared instance for all connections to the same authority,
	 * should be thread-safe.
	 */
	private S3Service service;
	
	/**
	 * s3://access_key_id:secret_access_key@hostname:port/
	 * 
	 * - hostname does not include bucket name
	 */
	private URI baseUri;

	/**
	 * @param authority
	 */
	protected PooledS3Connection(S3Authority authority) {
		super(authority);
		
		URI uri = authority.getUri();
		StringBuilder sb = new StringBuilder();
		sb.append(uri.getScheme()).append("://");
		sb.append(uri.getRawAuthority()).append('/'); // do not decode escape sequences
		this.baseUri = URI.create(sb.toString());
	}
	
	@Override
	public boolean isOpen() {
		return (service != null);
	}
	
	public void init() throws IOException {
		this.service = createService(authority);
	}

	@Override
	public void close() throws IOException {
		this.service = null;
	}

	public static String getAccessKey(Authority uri) {
		String userinfo = uri.getUserInfo();
		if (userinfo == null) {
			return "";
		}
		
		int colonPos = userinfo.indexOf(':');
		
		if (colonPos != -1) {
			return userinfo.substring(0, colonPos);
		} else {
			return userinfo;
		}
	}
	
	public static String getSecretKey(Authority uri) {
		String userinfo = uri.getUserInfo();
		if (userinfo == null) {
			return "";
		}
		
		int colonPos = userinfo.indexOf(':');
		
		if (colonPos != -1) {
			try {
				return URLDecoder.decode(userinfo.substring(colonPos + 1), "UTF-8");
			} catch (UnsupportedEncodingException e) {
				return userinfo.substring(colonPos + 1);
			}
		} else {
			return "";
		}
	}

	private S3Service createService(Authority authority) throws IOException {
		String accessKey = getAccessKey(authority);
		String secretKey = getSecretKey(authority);

		AWSCredentials credentials = new AWSCredentials(accessKey, secretKey);
		
		Jets3tProperties properties = new Jets3tProperties();
		properties.loadAndReplaceProperties(DEFAULTS, "defaults");
		
		properties.setProperty(S3SERVICE_S3_ENDPOINT, authority.getHost());
		int port = authority.getPort();
		if (port > -1) {
			String portStr = String.valueOf(port);
			properties.setProperty(S3SERVICE_S3_ENDPOINT_HTTP_PORT, portStr);
			properties.setProperty(S3SERVICE_S3_ENDPOINT_HTTPS_PORT, portStr);
		}
		// TODO https
		
		try {
			return new RestS3Service(credentials, null, null, properties);
		} catch (S3ServiceException e) {
			throw new IOException(e);
		}
	}
	
	/**
	 * Returns the {@link S3Service} instance.
	 *  
	 * @return {@link S3Service}
	 */
	public S3Service getService() {
		return service;
	}
	
	/**
	 * Returns the base URI of the connection.
	 * 
	 * @return base URI
	 */
	public URI getBaseUri() {
		return baseUri;
	}
	
	public InputStream getInputStream(String bucketName, String key) throws IOException {
		try {
			S3Object object = service.getObject(bucketName, key);
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
						returnToPool();
					}
				}
			};
			return is;
		} catch (Exception e) {
			returnToPool();
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
	public OutputStream getOutputStream(final String bucketName, final String key) throws IOException {
		try {
			final File tempFile = IAuthorityProxy.getAuthorityProxy(ContextProvider.getGraph()).newTempFile("cloveretl-amazons3-buffer", -1);
			
			OutputStream os = new FilterOutputStream(new FileOutputStream(tempFile)) {
				
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
								S3Bucket s3bucket = new S3Bucket(bucketName); 
								
								S3Object uploadObject;
								try {
									uploadObject = new S3Object(s3bucket, tempFile);
								} catch (NoSuchAlgorithmException e) {
									throw new IOException(e);
								}
								uploadObject.setKey(key);
								
								if (tempFile.length() <= MultipartUtils.MAX_OBJECT_SIZE) {
									try {
										service.putObject(s3bucket, uploadObject);
									} catch (S3ServiceException e) {
										throw getIOException(e);
									}
								} else {
									// CLO-4724:
									try {
										MultipartUtils mpUtils = new MultipartUtils(MultipartUtils.MAX_OBJECT_SIZE);
										mpUtils.uploadObjects(bucketName, service, Arrays.asList((StorageObject) uploadObject),
											    null // eventListener : Provide one to monitor the upload progress
										);
									} catch (S3ServiceException e) {
										throw getIOException(e);
									} catch (IOException e) {
										throw e;
									} catch (Exception e) {
										throw new IOException("Multi-part file upload failed", e);
									}
								}
							} finally {
								returnToPool();
								tempFile.delete();
							}
						}
					}
				}
				
			};
			
			return os;
		} catch (Exception e) {
			returnToPool();
			throw getIOException(e);
		}
	}
	
}
