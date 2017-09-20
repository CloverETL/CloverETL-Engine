package org.jetel.component.doc;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import junit.framework.TestCase;

public class DocumentationLinksTest extends TestCase {
	
	private static final String bouncyLink = "http://www.bouncycastle.org/latest_releases.html";
	private static final String bcprovLink = "http://repo2.maven.org/maven2/org/bouncycastle/bcprov-jdk15on/1.57/bcprov-jdk15on-1.57.jar";
	private static final String bcpkixLink = "http://repo2.maven.org/maven2/org/bouncycastle/bcpkix-jdk15on/1.57/bcpkix-jdk15on-1.57.jar";
	
	public void testLinks() throws Exception {
		assertTrue(checkURL(bouncyLink));
		assertTrue(checkURL(bcprovLink));
		assertTrue(checkURL(bcpkixLink));
	}
	
	private boolean checkURL(String url) {
		URL testUrl = null;
		
		try {  
			testUrl = new URL(url);
			URLConnection conn = testUrl.openConnection();
		    conn.connect();
	    } catch (MalformedURLException e) {  
	        return false;  
	    } catch (IOException e) {
	    	return false;
	    }

		try {  
	        testUrl.toURI();  
	    } catch (URISyntaxException e) {  
	        return false;  
	    }  
		
		return true;
	}
}
