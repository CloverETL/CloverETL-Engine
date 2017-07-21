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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.enums.ArchiveType;
/**
 * Helper class with some useful methods regarding file string manipulation
 *
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public class FileURLParser {

	// for embedded source
	//     "something   :       (         something   )       [[#|/]something]?
	//      ([^:]*)     (:)     (\\()     (.*)        (\\))   (((#)(.*))|($))
	private final static Pattern INNER_SOURCE = Pattern.compile("(([^:]*)([:])([\\(]))(.*)(\\))((((#)|(//))(.*))|($))");

	// constants
	private final static String ARCHIVE_ANCHOR = "#";
	private final static String DOUBLE_DOT_DEL = ":";
	private final static String BACK_SLASH = "\\";
	private final static String FORWARD_SLASH = "/";
	private final static String FILE = "file";
	private final static String ZIP_DDOT = ArchiveType.ZIP.getId()+DOUBLE_DOT_DEL;
	private final static String GZIP_DDOT = ArchiveType.GZIP.getId()+DOUBLE_DOT_DEL;
	private final static String TAR_DDOT = ArchiveType.TAR.getId()+DOUBLE_DOT_DEL;
	private final static String TGZ_DDOT = ArchiveType.TGZ.getId()+DOUBLE_DOT_DEL;
	
	/**
	 * Finds embedded source.
	 * 
	 * Example: 
	 * 		source:      zip:(http://linuxweb/~jausperger/employees.dat.zip)#employees0.dat
	 *      result: (g1) zip:
	 *              (g2) http://linuxweb/~jausperger/employees.dat.zip
	 *              (g3) #employees0.dat
	 * 
	 * @param source - input/output source
	 * @return matcher or null
	 */
	public static Matcher getURLMatcher(String source) {
		Matcher matcher = INNER_SOURCE.matcher(source);
		return matcher.find() ? matcher : null;
	}
	
	/**
	 * Adds final slash to the directory path, if it is necessary.
	 * @param directoryPath
	 * @return
	 */
	public static String appendSlash(String directoryPath) {
		if(directoryPath.length() == 0 || directoryPath.endsWith(FORWARD_SLASH) || directoryPath.endsWith(BACK_SLASH)) {
			return directoryPath;
		} else {
			return directoryPath + FORWARD_SLASH;
		}
	}
	
	/**
	 * Gets parent directory.
	 * @param input
	 * @return
	 */
	public static String getParentDirectory(String input) {
		int i;
		if((i = input.lastIndexOf(FORWARD_SLASH)) > 0 || (i = input.lastIndexOf(BACK_SLASH)) > 0) {
			return input.substring(0, i);
		}
		return input;
	}

	
	/**
	 * Gets the most inner url address.
	 * @param contextURL 
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static String getMostInnerAddress(String input) {
        // get inner input
        String innerInput = getInnerAddress(input);
        return innerInput == null ? input : getMostInnerAddress(innerInput);
	}
	
	/**
	 * Gets server or simple path (no proxy).
	 * @param input
	 * @return
	 */
	public static String getSimplePathOrServer(String input) {
		if (!isArchiveURL(input)) return input;
		
        // get inner input
        String innerInput = getInnerAddress(input);
        return innerInput == null ? input : getSimplePathOrServer(innerInput);
	}

	/**
	 * Gets inner url address. 
	 * @param input
	 * @return
	 */
	public static String getInnerAddress(String input) {
		if (input == null) return null;
		
		// get inner source
		Matcher matcher = getURLMatcher(input);
		String innerSource;
		if (matcher != null && (innerSource = matcher.group(5)) != null) {
			return innerSource;
		}
		
		// get inner source for archive files 
        StringBuilder innerInput = new StringBuilder();
        ArchiveType archiveType = getArchiveType(input, innerInput, new StringBuilder());
		return archiveType == null ? null : stripProtocol(innerInput.toString());
	}

	/**
	 * Gets string url without protocol.
	 * @param sURL
	 * @return
	 */
	private static String stripProtocol(String sURL) {
		return sURL.substring(sURL.indexOf(DOUBLE_DOT_DEL)+1);
	}
	
	/**
	 * Returns true if the URL is server URL.
	 * @param sUrl
	 * @return
	 * @throws MalformedURLException 
	 */
	public static boolean isServerURL(String sUrl) throws MalformedURLException {
		URL url = FileUtils.getFileURL(getMostInnerAddress(sUrl));
		return isServerURL(url);
	}
	
	/**
	 * Returns true if the URL is server URL.
	 * @param sUrl
	 * @return
	 * @throws MalformedURLException 
	 */
	public static boolean isServerURL(URL url) {
		return !(url.getProtocol().equals(FILE) || ArchiveType.fromString(url.getProtocol()) != null);
	}

	
	/**
	 * Returns true if the URL is archive URL.
	 * @param sUrl
	 * @return
	 */
	public static boolean isArchiveURL(String sUrl) {
		return sUrl.startsWith(ZIP_DDOT) || 
		       sUrl.startsWith(GZIP_DDOT) || 
		       sUrl.startsWith(TAR_DDOT) || 
		       sUrl.startsWith(TGZ_DDOT);
	}
	
	/**
	 * Returns true if the URL is file URL.
	 * @param sUrl
	 * @return
	 * @throws MalformedURLException
	 */
	public static boolean isFileURL(String sUrl) throws MalformedURLException {
		if (isArchiveURL(sUrl)) return false;
		URL url = FileUtils.getFileURL(sUrl);
		return isFileURL(url);
	}

	/**
	 * Returns true is the URL is file URL.
	 * @param url
	 * @return
	 * @throws MalformedURLException
	 */
	public static boolean isFileURL(URL url) {
		return url.getProtocol().equals(FILE);
	}

	/**
	 * Gets archive anchor.
	 * @param sURL
	 * @return
	 */
	public static String getAnchor(String sURL) {
		// get inner source
		Matcher matcher = getURLMatcher(sURL);
		if (matcher != null && (matcher.group(5)) != null) {
			sURL = matcher.group(2) + matcher.group(3) + matcher.group(7);
		}
		
		try {
			URL url = new URL(null, sURL, new GeneralHandler());
			sURL = url.getFile() + "#" + url.getRef();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
        if(sURL.contains(ARCHIVE_ANCHOR)) { 
        	return sURL.substring(sURL.indexOf(ARCHIVE_ANCHOR) + 1);
        }
    	return null;
	}
	
	/**
	 * Parses all protocols.
	 * @author admin (info@cloveretl.com)
	 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
	 *
	 * @created 11.1.2010
	 */
	private static class GeneralHandler extends URLStreamHandler {

		@Override
		public URLConnection openConnection(URL url) throws IOException {
			return null;
		}
		
	    @Override
		protected void parseURL(URL u, String spec, int start, int limit) {
	    	super.parseURL(u, spec, start, limit);
	    }
	}

	/**
	 * Gets file without archive anchor.
	 * @param sURL
	 * @return
	 */
	public static String getFileWithoutAnchor(String sURL) {
        if(sURL.contains(ARCHIVE_ANCHOR)) { 
        	return sURL.substring(sURL.indexOf(DOUBLE_DOT_DEL) + 1, sURL.lastIndexOf(ARCHIVE_ANCHOR));
        }
    	return sURL.substring(sURL.lastIndexOf(DOUBLE_DOT_DEL) + 1);
	}

    /**
     * Gets archive type.
     * @param input - input file
     * @param innerInput - output parameter
     * @param anchor - output parameter
     * @return
     */
    public static ArchiveType getArchiveType(String input, StringBuilder innerInput, StringBuilder anchor) {
    	// result value
    	ArchiveType archiveType = null;
    	
        //resolve url format for zip files
    	if (input.startsWith(ZIP_DDOT)) archiveType = ArchiveType.ZIP;
    	else if (input.startsWith(TAR_DDOT)) archiveType = ArchiveType.TAR;
    	else if (input.startsWith(GZIP_DDOT)) archiveType = ArchiveType.GZIP;
    	else if (input.startsWith(TGZ_DDOT)) archiveType = ArchiveType.TGZ;
    	
    	// parse the archive
        if((archiveType == ArchiveType.ZIP) || (archiveType == ArchiveType.TAR || archiveType == ArchiveType.GZIP || archiveType == ArchiveType.TGZ)) {
        	String sTmp;
        	if ((sTmp = getAnchor(input)) != null) anchor.append(sTmp);
        	innerInput.append(getFileWithoutAnchor(input));
        }
        
        // if doesn't exist inner input, inner input is input
        if (innerInput.length() == 0) innerInput.append(input);
        
        return archiveType;
    }

    /**
     * Gets outer address without inner address.
     * @param input
     * @return
     */
    public static String getOuterAddress(String input) {
    	// for out(inner)er
		Matcher matcher = getURLMatcher(input);
		if (matcher != null && (matcher.group(5)) != null) {
			return matcher.group(2) + matcher.group(3) + matcher.group(7);
		}
		// only archive may contain inner address
		if (!isArchiveURL(input)) return input;
		
		// parse the archive
		StringBuilder sb = new StringBuilder();
		getArchiveType(input, sb, new StringBuilder());
		return sb.toString();
    }
    
}
