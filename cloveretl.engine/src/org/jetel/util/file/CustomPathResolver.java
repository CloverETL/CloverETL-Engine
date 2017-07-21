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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * This class is intended to be implemented by third party applications. An implementation of this interface
 * can be passed to FileUtils class via addCustomPathResolver() method. Then all external resources are loaded through
 * this resolver: external metadata, external connections, input/output data files (fileURL attribute), 
 * imported CTL libraries, ... If null is returned the default clover implementation is used.
 *  
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 6.4.2010
 */
public interface CustomPathResolver {

	/**
	 * Method should return input stream corresponding 
	 * to given relative path and home directory specified in contextURL attribute.
	 * If null is returned the other CustomPathResolver implementation are used and finally if none of them has
	 * success the default clover implementation is used.
	 * @param contextURL working/home directory
	 * @param input URL of the source file
	 * @return
	 */
	public InputStream getInputStream(URL contextURL, String input) throws IOException;

	/**
	 * Method should return output stream corresponding
	 * to given relative path and home directory specified in contextURL attribute.
	 * If null is returned the other CustomPathResolver implementation are used and finally if none of them has
	 * success the default clover implementation is used.
	 * @param contextURL working/home directory
	 * @param output URL of the target file
	 * @return
	 */
	public OutputStream getOutputStream(URL contextURL, String output, boolean appendData, int compressLevel) throws IOException;
	
	
	
	/**
	 * Method should return URL object with proper URLStreamHandler &amp; URLConnection allowing to process openStream() and openConnection() methods 
	 * 
	 * @param contextURL working/home directory
	 * @param input URL of the source file
	 * @return
	 * @throws MalformedURLException - thrown when this resolver does not support protocol defined in fileURL
	 */
	public URL getURL(URL contextURL, String fileURL) throws MalformedURLException;
	
	
	
	/**
	 * Method should return true if this CustomPathResolver handles specified URL - protocol
	 * <p>
	 * Note that if the method returns <code>true</code>,
	 * the resolver must implement both
	 * {@link #getInputStream(URL, String)}
	 * and {@link #getOutputStream(URL, String, boolean, int)}.
	 * </p>
	 * 
	 * @param contextURL working/home directory
	 * @param fileURL URL of the source file
	 * @return
	 */
	public boolean handlesURL(URL contextURL, String fileURL);
	
	
	/**
	 * Method should resolve wildcard URL - i.e. return list of concrete files/URLs
	 * 
	 * @param contextURL working/home directory
	 * @param fileURL
	 * @return list of resolved filenames 
	 * @throws IOException if the the wildcard can't be resolved for whatever reason
	 */
	public List<String> resolveWildcardURL(URL contextURL, String fileURL) throws IOException;

}
