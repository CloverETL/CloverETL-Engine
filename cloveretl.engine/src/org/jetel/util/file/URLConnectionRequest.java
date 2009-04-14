package org.jetel.util.file;

import java.io.IOException;
import java.net.URLConnection;

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
            uc.setRequestProperty(authorizationType, URL_CONNECTION_BASIC + encode(userInfo));
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
}
