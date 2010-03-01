package org.jetel.util.file;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLConnection;
import java.net.URLDecoder;

import sun.misc.BASE64Encoder;

/**
 * 
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) OpenSys (www.opensys.eu)
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
            uc.setRequestProperty(authorizationType, URL_CONNECTION_BASIC + encode(decodeString(userInfo)));
        }
        return uc;
    }
    
    /**
     * Encodes the string.
     * @param source
     * @return
     */
    private static String encode(String source){
    	BASE64Encoder enc = new sun.misc.BASE64Encoder();
    	return enc.encode(source.getBytes());
    }
    
	/**
	 * Decodes string.
	 * @param s
	 * @return
	 */
	private static final String decodeString(String s) {
		try {
			return URLDecoder.decode(s, ENCODING);
		} catch (UnsupportedEncodingException e) {
			return s;
		}
	}
}
