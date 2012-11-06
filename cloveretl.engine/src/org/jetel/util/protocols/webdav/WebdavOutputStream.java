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
package org.jetel.util.protocols.webdav;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.util.file.FileURLParser;

import com.googlecode.sardine.impl.SardineException;

public class WebdavOutputStream extends OutputStream {
	
	private static Log logger = LogFactory.getLog(WebdavOutputStream.class);

	/*
	 * Path (with directories) to output file. It begins with http or https followed by domain name and path to file separated by slash.
	 * This pattern is not for validation but for parsing directories.
	 */
	private static final Pattern SUBDIR_REGEXP = Pattern.compile("(http[s]?://[^/]+)/(.+)/([^/]+)$");
	
	private OutputStream os;
	private SardinePutThread sardineThread;
	
	public static String getUsername(URL url) throws UnsupportedEncodingException {
		String userInfo = url.getUserInfo();
		
		if (userInfo != null) {
            userInfo = URLDecoder.decode(userInfo, "UTF-8");
			int colon = userInfo.indexOf(':');
			if (colon == -1) {
				return userInfo;
			}
			else {
				return userInfo.substring(0, colon);
			}
		}
		else {
			return "";
		}
	}
	
	public static String getPassword(URL url) throws UnsupportedEncodingException {
		String userInfo = url.getUserInfo();
		
		if (userInfo != null) {
            userInfo = URLDecoder.decode(userInfo, "UTF-8");
			int colon = userInfo.indexOf(':');
			if (colon == -1) {
				return "";
			}
			else {
				return userInfo.substring(colon+1);
			}
		}
		else {
			return "";
		}		
	}
	
	public URL stripUserinfo(URL url) throws MalformedURLException {
		return new URL(url.getProtocol(), url.getHost(), url.getPort(), url.getFile());
	}
	
	public WebdavOutputStream(String url) throws IOException {
		String proxyString = null;
		Matcher matcher = FileURLParser.getURLMatcher(url);
		if (matcher != null && (proxyString = matcher.group(5)) != null) {
			url = matcher.group(2) + matcher.group(3) + matcher.group(7);
		}
		URL parsedUrl = new URL(url);
		String username = getUsername(parsedUrl);
		String password = getPassword(parsedUrl);
		String outputURL = url;

		PipedOutputStream pos = new PipedOutputStream();
		os = pos;
		
		InputStream is = new PipedInputStream(pos);
		
		try {
			outputURL = stripUserinfo(parsedUrl).toString();
		} catch (MalformedURLException e) {}
		
		sardineThread = new SardinePutThread(outputURL, is, username, password, proxyString);
		sardineThread.start();
	}
	
	class SardinePutThread extends Thread {
		private String URL;
		private String proxyString;
		private InputStream is;
		private String username;
		private String password;
		private Throwable error;
		
		SardinePutThread(String URL, InputStream is, String username, String password, String proxyString) {
			this.URL = URL;
			this.is = is;
			this.username = username;
			this.password = password;
			this.proxyString = proxyString;
		}
		
		public Throwable getError() {
			return error;
		}
		
		/*
		 * Returns list of paths to each directory which is part of a path to a file.
		 * 
		 * E.g. for inputs (http://google.com, upload/ONE/TWO) returns [http://google.com/upload,
		 * http://google.com/upload/ONE, http://google.com/upload/ONE/TWO]
		 */
		private List<String> getSubDirectoriesPaths(String domainName, String relativePathToFile) {
			String[] dirs = relativePathToFile.split("/");
			List<String> pathList = new LinkedList<String>();

			for (int dirCount = 0; dirCount < dirs.length; ++dirCount) {
				StringBuffer strBuf = new StringBuffer(domainName);
				for (int i = 0; i <= dirCount; ++i) {
					strBuf.append("/").append(dirs[i]);
				}
				pathList.add(strBuf.toString());
			}
			return pathList;
		}
		
		@Override
		public void run() {
			try {

				ProxyConfiguration proxyConfiguration = new ProxyConfiguration(proxyString);
				WebdavClientImpl sardine = new WebdavClientImpl(username, password, proxyConfiguration);
				
				// This is a workaround needed for example for writing to CloverETL Server.
				// It will avoid retry on non-repeatable request error.
				// Digest authorization will be performed on this request and then the PUT
				// method (where retry caused by authorization would fail) is already authorized.
				boolean targetFileExists = sardine.exists(URL);

				if (!targetFileExists) {
					//target file does not exists
					Matcher matcher = SUBDIR_REGEXP.matcher(URL);
					if (!matcher.matches()) {
						logger.warn("url:" + URL + " for storing file on webdav doesn't match regexp:\"" + SUBDIR_REGEXP.pattern() + "\". Skipping creating directories");
					} else {
						// expecting valid url
						String domain = matcher.group(1);
						String relativePathToFile = matcher.group(2);

						List<String> subDirectoriesPaths = getSubDirectoriesPaths(domain, relativePathToFile);
						//check whether all directories exists (try to create missing directories)
						for (String path : subDirectoriesPaths) {
							if (!sardine.dirExists(path)) { // does not exist
								try {
									sardine.createDirectory(path);
									logger.info("webdav directory:" + path + " created.");
								} catch (SardineException e) {
									logger.warn("Failed to create firectory " + path);
								}
							}
						}
					}
				}
				
				sardine.put(URL, is);
			} catch (SardineException e) {
				error = new IOException(URL + ": " + e.getStatusCode() + " " + e.getResponsePhrase(), e);
			} catch (Throwable e) {
				error = e;
			} finally {
				// Closes the input stream both on error or after a successful run.
				// If successful, the put() method has written all the data already, so the stream can be safely closed.
				if (is != null) {
					try {
						is.close();
					} catch (IOException ioe) {
						if (error == null) {
							error = ioe;
						}
					}
				}
			}
		}
	}

	@Override
	public void write(int b) throws IOException {
		try {
			os.write(b);
		} catch (IOException ioe) {
			processException(ioe);
		}
	}
	
	@Override
	public void close() throws IOException {
		try {
			os.close();
			processException(null);
		} catch (IOException ioe) {
			processException(ioe);
		}
	}
	
	@Override
	public void flush() throws IOException {
		try {
			os.flush();
		} catch (IOException ioe) {
			processException(ioe);
		}
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		try {
			os.write(b);
		} catch (IOException ioe) {
			processException(ioe);
		}
	}
	
	/**
	 * Waits for the sardine thread to die
	 * and extracts the exception from it.
	 * 
	 * If there is one, throws it instead of the passed exception.
	 * Otherwise, throws the passed exception.
	 * 
	 * @param ioe
	 * @throws IOException
	 */
	private void processException(IOException ioe) throws IOException {
		try {
			sardineThread.join();
		} catch (InterruptedException e) {
			if ((ioe == null) && (e != null)) {
				ioe = new IOException(e);
			}
		} finally {
			Throwable sardineError = sardineThread.getError();
			if (sardineError != null) {
				ioe = sardineError instanceof IOException ? (IOException) sardineError : new IOException(sardineError);
			}
		}
		
		if (ioe != null) {
			throw ioe;
		}
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		try {
			os.write(b, off, len);
		} catch (IOException ioe) {
			processException(ioe);
		}
	}
	

// dirExists() has been refactored and moved to WebdavClientImpl
	
//	private boolean dirExists(String url, String user, String pass) throws IOException {
//		
//		URL url2 = new URL(url);
//		org.apache.commons.httpclient.HttpClient client = new org.apache.commons.httpclient.HttpClient();
//		client.getState().setCredentials(new org.apache.commons.httpclient.auth.AuthScope(url2.getHost(), url2.getPort()), new org.apache.commons.httpclient.UsernamePasswordCredentials(user, pass));
//		org.apache.commons.httpclient.methods.GetMethod get = new org.apache.commons.httpclient.methods.GetMethod(url);
//		get.setDoAuthentication(true);
//		int status = client.executeMethod(get);
//		get.releaseConnection();
//		return status == org.apache.commons.httpclient.util.HttpURLConnection.HTTP_OK;
//	}
}
