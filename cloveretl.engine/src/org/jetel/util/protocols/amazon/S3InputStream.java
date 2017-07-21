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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import org.jetel.util.file.WcardPattern;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

public class S3InputStream extends InputStream {
	
	private InputStream is;
	
	private S3InputStream(InputStream is) {
		this.is = is;
	}
	
	public static boolean isS3File(String url) {
		try {
			return isS3File(new URL(url));
		}
		catch (MalformedURLException e) {
			return false;
		}
	}
	
	public static boolean isS3File(URL url) {
		String hostname = url.getHost();
		return hostname.endsWith(org.jets3t.service.Constants.S3_DEFAULT_HOSTNAME);
	}
	
	public static String getAccessKey(URL url) {
		String userinfo = url.getUserInfo();
		if (userinfo == null) {
			return "";
		}
		
		int colonPos = userinfo.indexOf(':');
		
		if (colonPos != -1) {
			return userinfo.substring(0, colonPos);
		}
		else {
			return userinfo;
		}
	}
	
	public static String getSecretKey(URL url) {
		String userinfo = url.getUserInfo();
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
		}
		else {
			return "";
		}
	}
	
	public static String getBucket(URL url) {
		String host = url.getHost();
		
		if (!host.endsWith(org.jets3t.service.Constants.S3_DEFAULT_HOSTNAME)) {
			throw new IllegalArgumentException("Not an Amazon S3 host");
		}
		else if (host.equals(org.jets3t.service.Constants.S3_DEFAULT_HOSTNAME)) {
			throw new IllegalArgumentException("No bucket name specified in the host address.");
		}
		else {
			int hostLen = host.length();
			int dividingDotPos = hostLen - org.jets3t.service.Constants.S3_DEFAULT_HOSTNAME.length();
			
			return host.substring(0, dividingDotPos - 1);
		}
	}
	
	public static List<String> getObjects(URL url) throws IOException {
		List<String> objects = new ArrayList<String>();
		
		String filePattern = url.getFile();
		if (filePattern.startsWith("/")) {
			filePattern = filePattern.substring(1);
		}
		
		String accessKey = getAccessKey(url);
		String secretKey = getSecretKey(url);
		
		AWSCredentials credentials = new AWSCredentials(accessKey, secretKey);
		RestS3Service service;
		try {
			service = new RestS3Service(credentials);
			String bucket = getBucket(url);
			S3Object[] s3objects = service.listObjects(bucket);
			
			for (S3Object s3object : s3objects) {
				String key = s3object.getKey();
				if (WcardPattern.checkName(filePattern, key)) {
					String wholeURL = url.getProtocol() + "://"
						+ (url.getUserInfo() != null ? (url.getUserInfo() + "@") : "")
						+ url.getHost()
						+ (url.getPort() > 0 ? (":" + url.getPort()) : "")
						+ "/"
						+ key;
					objects.add(wholeURL);
				}
			}
			
		} catch (S3ServiceException e) {
			throw new IOException(e);
		}
		
		return objects;
	}
	
	public S3InputStream(URL url) throws IOException {
		this(url, null);
	}
	
	public S3InputStream(URL url, Proxy proxy) throws IOException {
		if (!isS3File(url)) {
			throw new IllegalArgumentException("Not an Amazon S3 host");
		}
		
		String accessKey = getAccessKey(url);
		String secretKey = getSecretKey(url);
		
		if (accessKey.isEmpty() || secretKey.isEmpty()) {
			URLConnection connection = proxy != null ? url.openConnection(proxy) : url.openConnection();
			is = connection.getInputStream();
		}
		else {
			AWSCredentials credentials = new AWSCredentials(accessKey, secretKey);
			RestS3Service service;
			try {
				service = new RestS3Service(credentials);
			} catch (S3ServiceException e) {
				throw new IOException(e);
			}
			
			String bucket = getBucket(url);
			String file = url.getFile();
			if (file.startsWith("/")) {
				file = file.substring(1);
			}
			
			S3Object getobject;
			
			try {
				getobject = service.getObject(bucket, file);
			} catch (S3ServiceException e) {
				throw new IOException(e);
			}
			
			try {
				is = getobject.getDataInputStream();
			} catch (ServiceException e) {
				throw new IOException(e);
			}
		}
	}

	@Override
	public int read() throws IOException {
		return is.read();
	}
	
	@Override
	public int available() throws IOException {
		return is.available();
	}
	
	@Override
	public void close() throws IOException {
		is.close();
	}
	
	@Override
	public boolean equals(Object obj) {
		return is.equals(obj);
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		return new S3InputStream(is);
	}
	
	@Override
	protected void finalize() throws Throwable {
		is.close();
	}
	
	@Override
	public boolean markSupported() {
		return is.markSupported();
	}
	
	@Override
	public synchronized void mark(int readlimit) {
		is.mark(readlimit);
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		return is.read(b);
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return is.read(b, off, len);
	}
	
	@Override
	public synchronized void reset() throws IOException {
		is.reset();
	}
	
	@Override
	public long skip(long n) throws IOException {
		return is.skip(n);
	}
}
