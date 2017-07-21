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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;

import org.jetel.component.fileoperation.Info.Type;
import org.jetel.component.fileoperation.SimpleParameters.CopyParameters;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.SimpleParameters.DeleteParameters;
import org.jetel.component.fileoperation.SimpleParameters.FileParameters;
import org.jetel.component.fileoperation.SimpleParameters.InfoParameters;
import org.jetel.component.fileoperation.SimpleParameters.ListParameters;
import org.jetel.component.fileoperation.SimpleParameters.MoveParameters;
import org.jetel.component.fileoperation.SimpleParameters.ReadParameters;
import org.jetel.component.fileoperation.SimpleParameters.ResolveParameters;
import org.jetel.component.fileoperation.SimpleParameters.WriteParameters;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.3.2012
 */
public class URLOperationHandler implements IOperationHandler {
	
	public static final int TIMEOUT = 1000;
	
	private URLConnection getConnection(URI uri) throws IOException {
		try {
			URL url = uri.toURL();
			URLConnection connection = url.openConnection();
			connection.setDoInput(false);
			connection.setDoOutput(false);
			return connection;
		} catch (Exception ex) {
			throw new IOException(FileOperationMessages.getString("URLOperationHandler.connection_failed"), ex); //$NON-NLS-1$
		}
	}
	
	public static class URLContent implements Content {
		
		protected final URI uri;
		
		private final Proxy proxy;
		
		private final String proxyUserInfo;
		
		public URLContent(URI uri) {
			String input = uri.toString();
			
	        // get inner source
			Matcher matcher = FileURLParser.getURLMatcher(input);
			String innerSource;
			Proxy proxy = null;
			String userInfo = null;
			if (matcher != null && (innerSource = matcher.group(5)) != null) {
				// get and set proxy and go to inner source
				proxy = FileUtils.getProxy(innerSource);
				input = matcher.group(2) + matcher.group(3) + matcher.group(7);
				if (proxy != null) {
					try {
						userInfo = new URI(innerSource).getUserInfo();
					} catch (URISyntaxException ex) {
					}
				}
			}
			
			this.proxy = proxy;
			this.proxyUserInfo = userInfo;
			this.uri = (proxy == null) ? uri : URI.create(input);
		}

		@Override
		public ReadableByteChannel read() throws IOException {
			URL url = uri.toURL();
			URLConnection connection = (proxy == null) ? FileUtils.getAuthorizedConnection(url) : FileUtils.getAuthorizedConnection(url, proxy, proxyUserInfo);
//			connection.setConnectTimeout(TIMEOUT);
//			connection.setReadTimeout(TIMEOUT);
			connection.setDoInput(true);
			connection.setDoOutput(false);
			return Channels.newChannel(connection.getInputStream());
		}

		@Override
		public WritableByteChannel write() throws IOException {
			URL url = uri.toURL();
			URLConnection connection = (proxy == null) ? FileUtils.getAuthorizedConnection(url) : FileUtils.getAuthorizedConnection(url, proxy, proxyUserInfo);
//			connection.setConnectTimeout(TIMEOUT);
//			connection.setReadTimeout(TIMEOUT);
			connection.setDoOutput(true);
			connection.setDoInput(false);
			return Channels.newChannel(connection.getOutputStream());
		}

		@Override
		public WritableByteChannel append() throws IOException {
			throw new IOException(FileOperationMessages.getString("IOperationHandler.append_not_supported")); //$NON-NLS-1$
		}
		
	}

	@Override
	public ReadableContent getInput(SingleCloverURI source, ReadParameters params) throws IOException {
		return new URLContent(source.toURI());
	}

	@Override
	public WritableContent getOutput(SingleCloverURI target, WriteParameters params) throws IOException {
		return new URLContent(target.toURI());
	}

	@Override
	public List<SingleCloverURI> resolve(SingleCloverURI uri, ResolveParameters params) throws IOException {
		return Arrays.asList(uri);
	}

	@Override
	public List<Info> list(SingleCloverURI target, ListParameters params) throws IOException {
		URI uri = target.toURI();
		Info info = info(uri);
		if (info == null) {
			throw new FileNotFoundException(uri.toString());
		}
		return Arrays.asList(info); 
	}
	
	private Info info(URI uri) throws IOException {
		URLConnection connection = getConnection(uri);
		String name = URIUtils.getFileName(uri);
		SimpleInfo info = new SimpleInfo(name, uri);
		info.setType(Type.FILE);
//		connection.setConnectTimeout(TIMEOUT); // Some connections would hang on the following attempts
		long lastModified = connection.getLastModified();
		if (lastModified > 0) {
			info.setLastModified(new Date(lastModified));
		}
		long size = connection.getContentLength();
		if (size >= 0) {
			info.setSize(size);
		}
		return info;
	}

	@Override
	public Info info(SingleCloverURI target, InfoParameters params) throws IOException {
		return info(target.toURI());
	}

	@Override
	public SingleCloverURI create(SingleCloverURI target, CreateParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public SingleCloverURI copy(SingleCloverURI source, SingleCloverURI target, CopyParameters params) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SingleCloverURI move(SingleCloverURI source, SingleCloverURI target, MoveParameters params) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SingleCloverURI delete(SingleCloverURI target, DeleteParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public File getFile(SingleCloverURI uri, FileParameters params) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getPriority(Operation operation) {
		return Integer.MIN_VALUE;
	}

	@Override
	public boolean canPerform(Operation operation) {
		switch (operation.kind) {
			case READ:
			case WRITE:
			case RESOLVE:
			case INFO:
			case LIST:
				return true;
			default: 
				return false;
		}
	}

	@Override
	public String toString() {
		return "URLOperationHandler"; //$NON-NLS-1$
	}

}
