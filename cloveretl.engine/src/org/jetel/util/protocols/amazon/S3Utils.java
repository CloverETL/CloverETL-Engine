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
package org.jetel.util.protocols.amazon;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.jetel.util.ExceptionUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1. 10. 2015
 */
public class S3Utils {
	
	private static String JETS3T_PROPERTIES_FILENAME = "jets3t.properties";
	
	private static String JETS3T_SERVER_SIDE_ENCRYPTION_PROPERTY = "s3service.server-side-encryption";
	
	private static Boolean sse = null;
	
	private S3Utils() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Thread-safe lazy initialization.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 7. 10. 2015
	 */
	private static class PropertiesSingletonHolder {
		private static final Properties INSTANCE = new Properties();
		
		static {
			try (InputStream cpIS = S3Utils.class.getResourceAsStream("/" + JETS3T_PROPERTIES_FILENAME);) {
				if (cpIS != null) {
					INSTANCE.load(cpIS);
				}
			} catch (IOException ioe) {}
		}
	}
	
	/**
	 * Backward compatibility: load SSE setting from old jets3t.properties
	 * file on classpath.
	 */
	private static boolean isSSE() {
		if (sse == null) {
			sse = Objects.equals("AES256", PropertiesSingletonHolder.INSTANCE.getProperty(JETS3T_SERVER_SIDE_ENCRYPTION_PROPERTY));
		}
		return sse;
	}

	/**
	 * CLO-8589: set content length to 0 to prevent a warning being logged.
	 * 
	 * @param service
	 * @param bucketName
	 * @param key
	 */
	public static void createEmptyObject(AmazonS3 service, String bucketName, String key) {
		ObjectMetadata metadata = createPutObjectMetadata();
		metadata.setContentLength(0);
		service.putObject(bucketName, key, new ByteArrayInputStream(new byte[0]), metadata);
	}
	
	public static void uploadFile(TransferManager tm, File file, String targetBucket, String targetKey) throws IOException {
		Upload upload = null;
		try {
			PutObjectRequest request = new PutObjectRequest(targetBucket, targetKey, file);
			if (isSSE()) {
				ObjectMetadata metadata = createPutObjectMetadata();
				request.withMetadata(metadata);
			}
			upload = tm.upload(request);
			upload.waitForCompletion();
//			if (file.length() <= MULTIPART_UPLOAD_THRESHOLD) {
//				try {
//					CopyObjectRequest copyRequest = new CopyObjectRequest(targetBucket, targetKey, targetBucket, targetKey);
//					copyRequest.withNewObjectMetadata(new ObjectMetadata());
//					// unsafe, we don't check the result
//					// the connection pool might abort the operation before it completes
//					tm.copy(copyRequest); // copy object to itself to update ETag
//				} catch (Exception ex) {
//					log.warn("Metadata update failed: " + targetBucket + "/" + targetKey, ex);
//				}
//			}
		} catch (AmazonClientException e) {
			throw ExceptionUtils.getIOException(e);
		} catch (InterruptedException e) {
			if (upload != null) {
				upload.abort(); // this is necessary!
			}
			throw ExceptionUtils.getIOException(e);
		}
	}

	/**
	 * CLO-7293:
	 * 
	 * Creates new {@link ObjectMetadata}, sets SSE algorithm, if configured.
	 * 
	 * @return new {@link ObjectMetadata}
	 */
	private static ObjectMetadata createPutObjectMetadata() {
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		return metadata;
	}

	public static ListObjectsRequest listObjectRequest(String bucketName, String prefix, String delimiter) {
		return new ListObjectsRequest(bucketName, prefix, null, delimiter, Integer.MAX_VALUE);
	}
	
	public static IOException getIOException(Throwable t) {
		if (t instanceof AmazonS3Exception) {
			AmazonS3Exception e = (AmazonS3Exception) t;
			Map<String, String> details = e.getAdditionalDetails();
			if (details != null) {
				String bucket = details.get("Bucket");
				String endpoint = details.get("Endpoint");
				// if selected endpoint does not match bucket location
				if ((e.getStatusCode() == HttpStatus.SC_MOVED_PERMANENTLY)
						&& !StringUtils.isEmpty(bucket)
						&& !StringUtils.isEmpty(endpoint)) {
					if (endpoint.startsWith(bucket + ".")) {
						endpoint = endpoint.substring(bucket.length() + 1);
					}
					t = new IOException(String.format("The bucket '%s' you are attempting to access must be addressed using the specified endpoint: '%s'.", bucket, endpoint), e);
				} else if (!details.isEmpty()) { // otherwise just append more details
					String message = e.getErrorMessage();
					e.setErrorMessage(message + "\nDetails: " + details);
				}
			}
		}
		return ExceptionUtils.getIOException(t);
	}
	
}
