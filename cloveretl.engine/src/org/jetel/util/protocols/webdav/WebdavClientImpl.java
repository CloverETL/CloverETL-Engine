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
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultProxyAuthenticationHandler;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.jetel.util.protocols.ProxyConfiguration;
import org.jetel.util.protocols.UserInfo;

import com.github.sardine.DavResource;
import com.github.sardine.impl.SardineException;
import com.github.sardine.impl.SardineImpl;
import com.github.sardine.impl.handler.ExistsResponseHandler;
import com.github.sardine.impl.handler.MultiStatusResponseHandler;
import com.github.sardine.impl.io.ConsumingInputStream;
import com.github.sardine.impl.methods.HttpPropFind;
import com.github.sardine.model.Allprop;
import com.github.sardine.model.Multistatus;
import com.github.sardine.model.Propfind;
import com.github.sardine.model.Response;
import com.github.sardine.util.SardineUtil;

/**
 * An extension of {@link SardineImpl}
 * with a few customizations.
 *  
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jan 10, 2013
 */
public class WebdavClientImpl extends SardineImpl implements WebdavClient {
	
	private static final String UTF_8 = "UTF-8";

    private AbstractHttpClient client;
    
	public WebdavClientImpl(URL url, ProxyConfiguration proxyConfiguration) throws UnsupportedEncodingException {
		this(url.getProtocol(), getUsername(url), getPassword(url), proxyConfiguration);
	}
	
	public WebdavClientImpl(String protocol, String username, String password, ProxyConfiguration proxyConfiguration) {
		this(username, password, proxyConfiguration);
		setDefaultProxyCredentials(protocol);
	}
	
	private WebdavClientImpl(String username, String password, ProxyConfiguration proxyConfiguration) {
		super(username, password, proxyConfiguration.getProxySelector());
		
		// prefer basic authentication, as NTLM does not seem to work
		// FIXME deprecated since 4.2, but ProxyAuthenticationStrategy does not support default scheme priority overriding
		client.setProxyAuthenticationHandler(new DefaultProxyAuthenticationHandler() {

			@Override
			protected List<String> getAuthPreferences(HttpResponse response, HttpContext context) {
	        	return Arrays.asList(
	        			AuthPolicy.BASIC,
	        			AuthPolicy.DIGEST,
	        			AuthPolicy.SPNEGO,
	        			AuthPolicy.NTLM);
			}
            
        });
		
		if (proxyConfiguration.isProxyUsed()) {
			UserInfo userInfo = proxyConfiguration.getUserInfo();
			if (!userInfo.isEmpty()) {
				HttpHost proxyHost = new HttpHost(proxyConfiguration.getHost(), proxyConfiguration.getPort());
				client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxyHost);
				setProxyCredentials(client, proxyHost.getHostName(), proxyHost.getPort(), userInfo.getUserInfo());
			}
		}
	}
	
	/**
	 * Combines <code>user</code> and <code>password</code> into a userInfo string "user:password".
	 * Handles <code>null</code> values.
	 * 
	 * @param user
	 * @param password
	 * @return
	 */
	public static String getUserInfo(String user, String password) {
		if ((user != null) && (password != null)) {
			return user + ":" + password;
		} else if (user != null) {
			return user;
		}
		
		return null;
	}
	
	/**
	 * This uses the nonstandard "http.proxyUser" and "http.proxyPassword"
	 * System properties.
	 * 
	 * Standard system properties (http.proxyHost, http.proxyPort)
	 * seem to work in {@link DefaultHttpClient}. If not,
	 * try replacing DefaultHttpClient with {@link SystemDefaultHttpClient}.
	 * 
	 * @param protocol
	 */
	private void setDefaultProxyCredentials(String protocol) {
		protocol = protocol.toLowerCase();
		String user = System.getProperty(protocol + ".proxyUser");
		String password = System.getProperty(protocol + ".proxyPassword");
		String proxyUserInfo = getUserInfo(user, password);
		String proxyHost = System.getProperty(protocol + ".proxyHost"); // null means any host
		String proxyPortString = System.getProperty(protocol + ".proxyPort");
		int proxyPort = -1; // -1 means any port
		if (proxyPortString != null) {
			proxyPort = Integer.parseInt(proxyPortString);
		}
		if (proxyUserInfo != null) {
			setProxyCredentials(client, proxyHost, proxyPort, proxyUserInfo);
		}
	}

	/**
	 * Stores the HTTP client instance so that client.setProxyAuthenticationHandler()
	 * can be called later.
	 */
	@Override
	protected AbstractHttpClient createDefaultClient(ProxySelector selector) {
		this.client = super.createDefaultClient(selector);
		return client;
	}

//	/**
//	 * Stores the HTTP client instance so that client.setProxyAuthenticationHandler()
//	 * can be called later.
//	 * 
//	 * {@link DefaultHttpClient} has been replaced with {@link SystemDefaultHttpClient}.
//	 */
//	@Override
//	protected AbstractHttpClient createDefaultClient(ProxySelector selector) {
//		HttpParams params = createDefaultHttpParams();
//		// used instead of DefaultHttpClient, should take system properties into account
//		AbstractHttpClient client = new SystemDefaultHttpClient(params); 
//		client.setRoutePlanner(createDefaultRoutePlanner(client.getConnectionManager().getSchemeRegistry(), selector));
//		this.client = client;
//		return client;
//	}
//	
//	
//	/**
//	 * Not used anymore, {@link SystemDefaultHttpClient} uses {@link PoolingClientConnectionManager}
//	 * instead of the deprecated {@link ThreadSafeClientConnManager}.
//	 * 
//	 * @deprecated 
//	 */
//	@Deprecated
//	@Override
//	protected ClientConnectionManager createDefaultConnectionManager(SchemeRegistry schemeRegistry) {
//		return super.createDefaultConnectionManager(schemeRegistry);
//	}

	private void setProxyCredentials(AbstractHttpClient client, String hostName, int port, String userInfo) {
		if (userInfo == null || hostName == null || port < 0) {
			return;
		}
		String username;
		String password;
        int atColon = userInfo.indexOf(':');
        if (atColon >= 0) {
            username = userInfo.substring(0, atColon);
            password = userInfo.substring(atColon + 1);
        } else {
            username = userInfo;
            password = null;
        }
		Credentials credentials = new UsernamePasswordCredentials(username, password);
		InetSocketAddress addr = new InetSocketAddress(hostName, 0); // the port number is not used
		String hostAddress = addr.getAddress().getHostAddress(); // convert hostname to IP
		
		client.getCredentialsProvider().setCredentials(new AuthScope(hostName, port, AuthScope.ANY_REALM, AuthPolicy.BASIC), credentials);
		client.getCredentialsProvider().setCredentials(new AuthScope(hostAddress, port, AuthScope.ANY_REALM, AuthPolicy.BASIC), credentials);
		client.getCredentialsProvider().setCredentials(new AuthScope(hostName, port, AuthScope.ANY_REALM, AuthPolicy.DIGEST), credentials);
		client.getCredentialsProvider().setCredentials(new AuthScope(hostAddress, port, AuthScope.ANY_REALM, AuthPolicy.DIGEST), credentials);
		
		// fallback
		credentials = new NTCredentials(username, password, "", "");
		client.getCredentialsProvider().setCredentials(new AuthScope(hostName, port, AuthScope.ANY_REALM, AuthPolicy.NTLM), credentials);
		client.getCredentialsProvider().setCredentials(new AuthScope(hostAddress, port, AuthScope.ANY_REALM, AuthPolicy.NTLM), credentials);
	}
	
	/**
	 * Execute PROPFIND with depth 0.
	 * 
	 * @param url
	 * @return
	 * @throws IOException
	 */
	// not used now, but may be useful for file operations
	@Override
	public DavResource info(String url) throws IOException {
		HttpPropFind entity = new HttpPropFind(url);
		entity.setDepth("0");
		Propfind body = new Propfind();
		body.setAllprop(new Allprop());
		entity.setEntity(new StringEntity(SardineUtil.toXml(body), UTF_8));
		
		Multistatus multistatus = execute(entity, new MultiStatusResponseHandler() {

			@Override
			public Multistatus handleResponse(HttpResponse response) throws SardineException, IOException {
				StatusLine statusLine = response.getStatusLine();
				int statusCode = statusLine.getStatusCode();
				if (statusCode == HttpStatus.SC_NOT_FOUND)
				{
					return null; // expected response, do not throw an exception
				}
				return super.handleResponse(response);
			}
			
		});
		
		if (multistatus == null) {
			return null;
		}
		
		List<Response> responses = multistatus.getResponse();
		List<DavResource> resources = new ArrayList<DavResource>(responses.size());
		for (Response resp : responses) {
			try {
				resources.add(new DavResource(resp));
			} catch (URISyntaxException e) {
				// Ignore resource with invalid URI
			}
		}
		return !resources.isEmpty() ? resources.get(0) : null;
	}

	/**
	 * @see com.googlecode.sardine.Sardine#get(java.lang.String, java.util.Map)
	 * 
	 * Just like the method above, but uses a custom response handler.
	 * Returns <code>null</code> if HTTP 404 (not found) is returned.
	 */
	public InputStream getIfExists(String url, Map<String, String> headers) throws IOException
	{
		HttpGet get = new HttpGet(url);
		for (String header : headers.keySet())
		{
			get.addHeader(header, headers.get(header));
		}
		// Must use #execute without handler, otherwise the entity is consumed
		// already after the handler exits.
		HttpResponse response = this.execute(get);
		ExistsResponseHandler handler = new ExistsResponseHandler() {

			@Override
			public Boolean handleResponse(HttpResponse response) throws SardineException {
				StatusLine statusLine = response.getStatusLine();
				int statusCode = statusLine.getStatusCode();
				if (statusCode == HttpStatus.SC_NOT_FOUND) {
					return false;
				}
				
				validateResponse(response);
				return true; // the validation has not thrown any exceptions
			}
			
		};
		try
		{
			if (!handler.handleResponse(response)) {
				return null; // HttpStatus.SC_NOT_FOUND = 404 
			}
			// Will consume the entity when the stream is closed.
			return new ConsumingInputStream(response);
		}
		catch (IOException ex)
		{
			get.abort();
			throw ex;
		}
	}

	/**
	 * Check whether remote directory exists.
	 * If HEAD used by {@link #exists(String)} fails, GET is used instead.
	 * 
	 * See <a href="http://code.google.com/p/sardine/issues/detail?id=48">http://code.google.com/p/sardine/issues/detail?id=48</a>
	 * and <a href="https://issues.alfresco.com/jira/browse/ALF-7883">https://issues.alfresco.com/jira/browse/ALF-7883</a> 
	 * for more information and motivation.
	 * 
	 * This method should work both for WebDAV and plain HTTP,
	 * hence PROPFIND can't be used.
	 * 
	 * @param url
	 *            Path to the directory.
	 * @return True if the directory exists.
	 * @throws IOException
	 */
	// CL-2709: The bug in Alfresco has already been fixed.
	// As for Jackrabbit, http://koule:22401/repository/ returns 404 both for GET and HEAD
	@Override
	public boolean dirExists(String url) throws IOException {
		try {
			return exists(url); // first try with HTTP HEAD
		} catch (SardineException se) {
			// https://issues.alfresco.com/jira/browse/ALF-7883
			switch (se.getStatusCode()) {
				case HttpStatus.SC_BAD_REQUEST: // HEAD failed
				case HttpStatus.SC_METHOD_NOT_ALLOWED: // HEAD not allowed
					// try HTTP GET as a fallback
					InputStream is = getIfExists(url, Collections.<String, String>emptyMap());
					if (is == null) {
						return false;
					} else {
						is.close();
						return true;
					}
			}

			throw se;
		}
	}

	public static String getUsername(URL url) throws UnsupportedEncodingException {
		String userInfo = url.getUserInfo();
		
		if (userInfo != null) {
            userInfo = URLDecoder.decode(userInfo, UTF_8);
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
            userInfo = URLDecoder.decode(userInfo, UTF_8);
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
	
}