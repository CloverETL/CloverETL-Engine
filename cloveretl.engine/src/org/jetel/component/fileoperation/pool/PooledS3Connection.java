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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.jetel.component.fileoperation.PrimitiveS3OperationHandler;
import org.jetel.component.fileoperation.URIUtils;
import org.jetel.util.protocols.URLValidator;
import org.jetel.util.protocols.Validable;
import org.jetel.util.protocols.amazon.S3Utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 3. 2015
 */
public class PooledS3Connection extends AbstractPoolableConnection implements Validable, URLValidator {

	/**
	 * Shared instance, do not modify!
	 */
	private static final ClientConfiguration DEFAULTS = PredefinedClientConfigurations.defaultConfig();
	
	/**
	 * Shared instance for all connections to the same authority,
	 * should be thread-safe.
	 */
	private AmazonS3Client service;
	
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
		return (service != null);
	}
	
	public void init() throws IOException {
		this.service = createService((S3Authority) authority);
		validate();
	}
	
	@Override
	public void validate() throws IOException {
		try {
			// validate connection
			service.listBuckets();
		} catch (AmazonClientException e) {
			if (e.getCause() instanceof IllegalArgumentException) {
				if ("Empty key".equals(e.getCause().getMessage())) {
					throw new IOException("S3 URL does not contain valid keys. Please supply access key and secret key in the following format: s3://<AccessKey:SecretKey>@s3.amazonaws.com/<bucket>", S3Utils.getIOException(e));
				}
			}
			throw new IOException("Connection validation failed", S3Utils.getIOException(e));
		}
	}

	@Override
	public void validate(URL url) throws IOException {
		validate();
		try {
			PrimitiveS3OperationHandler.listFiles(url.toURI(), this);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void close() throws IOException {
		IOException ioe = null;
		if (service != null) {
			try {
				service.shutdown();
			} catch (Exception e) {
				ioe = S3Utils.getIOException(e);
			} finally {
				this.service = null;
				if (transferManager != null) {
					try {
						transferManager.shutdownNow(false);
					} catch (Exception e) {
						if (ioe != null) {
							ioe.addSuppressed(e);
						} else {
							ioe = S3Utils.getIOException(e);
						}
					}
				}
			}
		}
		if (ioe != null) {
			throw ioe;
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

	private AmazonS3Client createService(S3Authority authority) {
		String accessKey = getAccessKey(authority);
		String secretKey = getSecretKey(authority);

		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		
		ClientConfiguration properties = new ClientConfiguration(DEFAULTS);
//		properties.setSignerOverride("AWSS3V4SignerType");
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials, properties);
		amazonS3Client.setEndpoint(authority.getHost());
//		amazonS3Client.setRegion(Region.EU_Frankfurt.toAWSRegion());
//		amazonS3Client.setSignerRegionOverride(Region.EU_Frankfurt.toString());

//		int port = authority.getPort();
//		if (port > -1) {
//			String portStr = String.valueOf(port);
//		}
		// TODO https
		
		return amazonS3Client;
	}
	
	/**
	 * Returns the {@link AmazonS3} instance.
	 *  
	 * @return {@link AmazonS3}
	 */
	public AmazonS3 getService() {
		return service;
	}
	
	private TransferManager transferManager;
	
	public TransferManager getTransferManager() {
		if (transferManager == null) {
			transferManager = new TransferManager(service);
			TransferManagerConfiguration config = new TransferManagerConfiguration();
//			config.setMultipartUploadThreshold(MULTIPART_UPLOAD_THRESHOLD);
//			config.setMinimumUploadPartSize(MULTIPART_UPLOAD_THRESHOLD);
			transferManager.setConfiguration(config);
		}
		return transferManager;
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
		return S3Utils.getInputStream(uri, this);
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

}
