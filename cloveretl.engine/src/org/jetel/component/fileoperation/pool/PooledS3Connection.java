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
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;

import org.jetel.component.fileoperation.PrimitiveS3OperationHandler;
import org.jets3t.service.Constants;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;

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
		return (service != null) && !service.isShutdown();
	}
	
	public void init() throws IOException {
		this.service = createService((S3Authority) authority);
	}

	@Override
	public void close() throws IOException {
		if (service != null) {
			try {
				service.shutdown();
			} catch (ServiceException e) {
				throw new IOException(e);
			} finally {
				this.service = null;
			}
		}
	}

	public static String getAccessKey(S3Authority authority) {
		String userinfo = authority.getUri().getRawUserInfo();
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
	
	public static String getSecretKey(S3Authority authority) {
		String userinfo = authority.getUri().getRawUserInfo();
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
		
		return new RestS3Service(credentials, null, null, properties);
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
	
}
