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
package org.jetel.util.file;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLConnection;
import java.net.URLDecoder;

import org.apache.commons.codec.binary.Base64;

/**
 * 
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public class URLConnectionRequest {

	// basic property
	private static final String URL_CONNECTION_BASIC = "Basic ";

	// general authorization
	public static final String URL_CONNECTION_AUTHORIZATION = "Authorization";

	// proxy authorization
	public static final String URL_CONNECTION_PROXY_AUTHORIZATION = "Proxy-Authorization";
	
	// standard encoding for URLDecoder
	// see http://www.w3.org/TR/html40/appendix/notes.html#non-ascii-chars
	private static final String ENCODING = "UTF-8";
	
	/**
	 * Creates an authorized connection.
	 * @param uc
	 * @param userInfo
	 * @param authorizationType
	 * @return
	 * @throws IOException
	 */
    public static URLConnection getAuthorizedConnection(URLConnection uc, String userInfo, String authorizationType) {
        // check authorization 
        if (userInfo != null) {
        	// FIXME does not work for SOCKS proxies
            uc.setRequestProperty(authorizationType, URL_CONNECTION_BASIC + encode(decodeString(userInfo)));
        }
        return uc;
    }
    
    /**
     * Encodes the string.
     * @param source
     * @return
     */
    static String encode(String source){
    	// CL-2434: the method pads the result with equal signs '=' correctly
    	// Base64.encodeBase64URLSafeString() does not pad the result with '=', which was causing the authentication to fail
    	
    	// commons-codec-1.4 uses multiline chunking in Base64.encodeBase64String(), 
    	// therefore Base64.encodeBase64String() has been replaced with the following: 
    	return org.apache.commons.codec.binary.StringUtils.newStringUtf8(Base64.encodeBase64(source.getBytes(), false));
    	// FIXME Since commons-codec-1.5 the code can be replaced with:
    	// return Base64.encodeBase64String(source.getBytes());
    }
    
	/**
	 * Decodes string.
	 * @param s
	 * @return
	 */
	static final String decodeString(String s) {
		try {
			return URLDecoder.decode(s, ENCODING);
		} catch (UnsupportedEncodingException e) {
			return s;
		}
	}
}
