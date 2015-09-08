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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.fileoperation.PrimitiveS3OperationHandler;
import org.jetel.component.fileoperation.URIUtils;
import org.jetel.util.protocols.Validable;
import org.jetel.util.stream.InterruptibleInputStream;
import org.jets3t.service.Constants;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.MultipartPart;
import org.jets3t.service.model.MultipartUpload;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.security.ProviderCredentials;
import org.jets3t.service.utils.MultipartUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 3. 2015
 */
public class PooledS3Connection extends AbstractPoolableConnection implements Validable {

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
		// TODO perform a connection test request?
		return (service != null) && !service.isShutdown();
	}
	
	public void init() throws IOException {
		this.service = createService((S3Authority) authority);
		validate();
	}
	
	@Override
	public void validate() throws IOException {
		try {
			// validate connection
			service.listAllBuckets();
		} catch (S3ServiceException e) {
			if (e.getCause() instanceof IllegalArgumentException) {
				if ("Empty key".equals(e.getCause().getMessage())) {
					throw new IOException("S3 URL does not contain valid keys. Please supply access key and secret key in the following format: s3://<AccessKey:SecretKey>@s3.amazonaws.com/<bucket>", PrimitiveS3OperationHandler.getIOException(e));
				}
			}
			throw new IOException("Connection validation failed", PrimitiveS3OperationHandler.getIOException(e));
		}
	}

	@Override
	public void close() throws IOException {
		if (service != null) {
			try {
				service.shutdown();
			} catch (ServiceException e) {
				throw PrimitiveS3OperationHandler.getIOException(e);
			} finally {
				this.service = null;
			}
		}
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			if (isBorrowed()) {
				// CLO-6688: leaked connections must be closed, as they may be already broken
				close();
			}
		} finally {
			super.finalize();
		}
	}

	public static String getAccessKey(S3Authority authority) {
		String userinfo = authority.getUri().getRawUserInfo();
		if (userinfo == null) {
			return "";
		}
		
		int colonPos = userinfo.indexOf(':');
		
		String accessKey = (colonPos != -1) ? userinfo.substring(0, colonPos) : userinfo;
		return URIUtils.urlDecode(accessKey);
	}
	
	public static String getSecretKey(S3Authority authority) {
		String userinfo = authority.getUri().getRawUserInfo();
		if (userinfo == null) {
			return "";
		}
		
		int colonPos = userinfo.indexOf(':');
		
		if (colonPos != -1) {
			return URIUtils.urlDecode(userinfo.substring(colonPos + 1));
		} else {
			return "";
		}
	}

	private S3Service createService(S3Authority authority) {
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
		
		return new CustomizedS3Service(credentials, properties);
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
	
	/**
	 * Returns an {@link InputStream} for reading from the specified {@link URI}.
	 * <p>
	 * <b>Calling this method passes ownership of the connection to the stream.</b>
	 * </p>
	 * 
	 * @param uri - source {@link URI}
	 * @return new {@link InputStream} that takes ownership of the connection
	 * @throws IOException
	 */
	public InputStream getInputStream(URI uri) throws IOException {
		return PrimitiveS3OperationHandler.getInputStream(uri, this);
	}

	/**
	 * Returns an {@link OutputStream} instance that writes data
	 * to a temp file, uploads it to S3 when the stream is closed
	 * and deletes the temp file.
	 * 
	 * If the file size exceeds 5 GB, performs multipart upload.
	 * <p>
	 * <b>Calling this method passes ownership of the connection to the stream.</b>
	 * </p>
	 * 
	 * @param uri - target {@link URI}
	 * @return new {@link OutputStream} that takes ownership of the connection
	 * @throws IOException
	 */
	public OutputStream getOutputStream(URI uri) throws IOException {
		return PrimitiveS3OperationHandler.getOutputStream(uri, this);
	}

	/*
	 * CustomizedS3Service overrides putObjectMaybeAsMultipart()
	 * to make multipart uploads interruptible.
	 * ------------------------------------------------------------------------
	 * Based on org.jets3t.service.impl.rest.httpclient.RestS3Service:
	 * 
	 * JetS3t : Java S3 Toolkit
	 * Project hosted at http://bitbucket.org/jmurty/jets3t/
	 *
	 * Copyright 2006-2011 James Murty
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
	private static class CustomizedS3Service extends RestS3Service {

	    private static final Log log = LogFactory.getLog(RestS3Service.class);

	    /**
		 * @param credentials
		 * @param jets3tProperties
		 */
		public CustomizedS3Service(ProviderCredentials credentials, Jets3tProperties jets3tProperties) {
			super(credentials, null, null, jets3tProperties);
		}

		/*
		 * Overridden to make multipart uploads interruptible. 
		 */
		@Override
		public void putObjectMaybeAsMultipart(String bucketName, StorageObject object, long maxPartSize)
				throws ServiceException {
	        // Only file-based objects are supported
	        if (object.getDataInputFile() == null) {
	            throw new ServiceException(
	                "multipartUpload method only supports file-based objects");
	        }

	        MultipartUtils multipartUtils = new MultipartUtils(maxPartSize) {

	        	/*
	        	 * Overridden to wrap all data input streams in InterruptibleInputStream.
	        	 */
				@Override
				public List<S3Object> splitFileIntoObjectsByMaxPartSize(String objectKey, File file)
						throws IOException, NoSuchAlgorithmException {
					List<S3Object> objects = super.splitFileIntoObjectsByMaxPartSize(objectKey, file);
					for (S3Object o: objects) {
						try {
							InputStream is = o.getDataInputStream();
							if (is != null) {
								o.setDataInputStream(new InterruptibleInputStream(is));
							}
						} catch (ServiceException e) {
							log.warn("Failed to set interruptible data input stream", e);
						}
					}
					return objects;
				}
	        	
	        };

	        // Upload object normally if it doesn't exceed maxPartSize
	        if (!multipartUtils.isFileLargerThanMaxPartSize(object.getDataInputFile())) {
	            log.debug("Performing normal PUT upload for object with data <= " + maxPartSize);
	            putObject(bucketName, object);
	        } else {
	            log.debug("Performing multipart upload for object with data > " + maxPartSize);

	            // Start upload
	            MultipartUpload upload = multipartStartUpload(bucketName, object.getKey(),
	                object.getMetadataMap(), object.getAcl(), object.getStorageClass());

	            // Ensure upload is present on service-side, might take a little time
	            boolean foundUpload = false;
	            int maxTries = 5; // Allow up to 5 lookups for upload before we give up
	            int tries = 0;
	            do {
	                try {
	                    multipartListParts(upload);
	                    foundUpload = true;
	                } catch (S3ServiceException e) {
	                    if ("NoSuchUpload".equals(e.getErrorCode())) {
	                        tries++;
	                        try {
	                            Thread.sleep(1000); // Wait for a second
	                        } catch (InterruptedException ie) {
	                            tries = maxTries;
	                        }
	                    } else {
	                        // Bail out if we get a (relatively) unexpected exception
	                        throw e;
	                    }
	                }
	            } while (!foundUpload && tries < maxTries);

	            if (!foundUpload) {
	                throw new ServiceException(
	                    "Multipart upload was started but unavailable for use after "
	                    + tries + " attempts, giving up");
	            }

	            // Will attempt to delete multipart upload upon failure.
	            try {
	                List<S3Object> partObjects = multipartUtils.splitFileIntoObjectsByMaxPartSize(
	                    object.getKey(), object.getDataInputFile());

	                List<MultipartPart> parts = new ArrayList<MultipartPart>();
	                int partNumber = 1;
	                for (S3Object partObject: partObjects) {
	                    MultipartPart part = multipartUploadPart(upload, partNumber, partObject);
	                    parts.add(part);
	                    partNumber++;
	                }

	                multipartCompleteUpload(upload, parts);

	                // Apply non-canned ACL settings if necessary (canned ACL will already be applied)
	                if (object.getAcl() != null
	                    && object.getAcl().getValueForRESTHeaderACL() == null)
	                {
	                    if (log.isDebugEnabled()) {
	                        log.debug("Completed multipart upload for object with a non-canned ACL"
	                            + " so an extra ACL Put is required");
	                    }
	                    putAclImpl(bucketName, object.getKey(), object.getAcl(), null);
	                }

	            } catch (RuntimeException e) {
	                throw e;
	            } catch (Exception e) {
	                // If upload fails for any reason after the upload was started, try to clean up.
	                log.warn("Multipart upload failed, attempting clean-up by aborting upload", e);
	                try {
	                    multipartAbortUpload(upload);
	                } catch (S3ServiceException e2) {
	                    log.warn("Multipart upload failed and could not clean-up by aborting upload", e2);
	                }
	                // Throw original failure exception
	                if (e instanceof ServiceException) {
	                    throw (ServiceException) e;
	                } else {
	                    throw new ServiceException("Multipart upload failed", e);
	                }
	            }
	        }
		}
	}
}
