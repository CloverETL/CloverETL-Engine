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
import java.net.MalformedURLException;
import java.net.URL;

import com.googlecode.sardine.Sardine;
import com.googlecode.sardine.SardineFactory;

public class WebdavOutputStream extends OutputStream {
	private OutputStream os;
	
	public static String getUsername(URL url) {
		String userInfo = url.getUserInfo();
		
		if (userInfo != null) {
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
	
	public static String getPassword(URL url) {
		String userInfo = url.getUserInfo();
		
		if (userInfo != null) {
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
		
		Thread thread = new SardinePutThread(outputURL, is, username, password);
		thread.start();
	}
	
	class SardinePutThread extends Thread {
		private String URL;
		private InputStream is;
		private String username;
		private String password;
		
		SardinePutThread(String URL, InputStream is, String username, String password) {
			this.URL = URL;
			this.is = is;
			this.username = username;
			this.password = password;
		}
		
		public void run() {
			try {
				Sardine sardine = SardineFactory.begin(username, password);
				sardine.put(URL, is);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void write(int b) throws IOException {
		os.write(b);
	}
	
	@Override
	public void close() throws IOException {
		os.close();
	}
	
	@Override
	public void flush() throws IOException {
		os.flush();
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		os.write(b);
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		os.write(b, off, len);
	}
}
