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
package org.jetel.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.fileoperation.CloverURI;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.Parser.DataSourceType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPort;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.WcardPattern;
import org.jetel.util.property.PropertyRefResolver;

/***
 * Supports a field/dictionary reading and reading from urls.
 * 
 * FYI alternative implementation of the same algorithm is in {@link SourceIterator}
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public class ReadableChannelIterator {
	
	// logger
    private static Log defaultLogger = LogFactory.getLog(ReadableChannelIterator.class);
    
    // default charset for port and dictionary reading
	public static final String DEFAULT_CHARSET = "UTF-8";

	// dictionary and port protocol prefix
	private static final String DICT = "dict:";
	private static final String PORT = "port:";
	private static final String PROTOCOL_FILE = "file";

	// all file URLs and a context for URLs.
	private String fileURL;
	private URL contextURL;
	private Iterator<String> filenameItor;
	private List<String> files; 

	// input port support
	private InputPort inputPort;
	private ReadableChannelPortIterator portReadingIterator;
	private PropertyRefResolver propertyRefResolve;				// for port reading - source mode

	// dictionary support
	private Dictionary dictionary;
	private ReadableChannelDictionaryIterator dictionaryReadingIterator;

	// charset for port or dictionary reading
	private String charset;

	// current source name
	private String currentFileName;
	
	
	// true if fileURL contains port or dictionary protocol
	private boolean isGraphDependentSource;

	// true if fileURL contains port protocol 
	private boolean bInputPort;

	// true if java.net.URI is preferred as a source provided by this iterator
	private DataSourceType preferredDataSourceType = DataSourceType.CHANNEL;

	// others
	private int firstPortProtocolPosition;
	private int firstDictProtocolPosition;
	private int currentPortProtocolPosition;
	private List<String> portProtocolFields;

	/**
	 * Constructor.
	 * 
	 * @param inputPort
	 * @param contextURL
	 * @param fileURL
	 */
	public ReadableChannelIterator(InputPort inputPort, URL contextURL, String fileURL) {
		this.inputPort = inputPort;
		this.fileURL = fileURL;
		this.contextURL = contextURL;
	}

	/**
	 * Checks this class for the first using.
	 */
	public void checkConfig() throws ComponentNotReadyException {
		common(true);
	}

	/**
	 * Initializes this class for the first using.
	 */
	public void init() throws ComponentNotReadyException {
		common(true);
	}
	
	private void common(boolean resolveAllFileNames) throws ComponentNotReadyException {
		// charset
		if (charset == null) charset = DEFAULT_CHARSET;
		
		// file iterator
		prepareFileIterator(resolveAllFileNames);
		
		// port iterator
		initPortFields();
		
		// dictionary iterator
		dictionaryReadingIterator = new ReadableChannelDictionaryIterator(dictionary);
		dictionaryReadingIterator.setCharset(charset);
		dictionaryReadingIterator.setContextURL(contextURL);
		
		// current state
		currentPortProtocolPosition = 0;
		currentFileName = "unknown";
		
		// graph dependency - indicators
		isGraphDependentSource = firstPortProtocolPosition == 0 || firstDictProtocolPosition == 0;
		bInputPort = portReadingIterator != null;
	}
	
	/**
	 * Resets this class for a next using.
	 */
	public void reset() {
		try {
			init();
		} catch (ComponentNotReadyException e) {
			defaultLogger.error(e); //never happened
		}
	}
	
	/**
	 * File iterator initialization.
	 * @throws ComponentNotReadyException
	 */
	private void prepareFileIterator(boolean resolveAllNames) throws ComponentNotReadyException {
		WcardPattern pat = new WcardPattern();
		pat.setParent(contextURL);
        pat.addPattern(fileURL, Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
        pat.resolveAllNames(resolveAllNames);
        try {
			files = pat.filenames();
		} catch (IOException e) {
			throw new ComponentNotReadyException("Can't prepare file list from wildcard URL", e);
		}
        firstPortProtocolPosition = getFirstProtocolPosition(files, PORT);
        firstDictProtocolPosition = getFirstProtocolPosition(files, DICT);
		if (firstPortProtocolPosition >= 0 && inputPort == null) 
			throw new ComponentNotReadyException("Input port is not defined for '" + files.get(firstPortProtocolPosition) + "'.");
		
        portProtocolFields = getAndRemoveProtocol(files, PORT, firstPortProtocolPosition);
        this.filenameItor = files.iterator();
	}

	/**
	 * Port reading initialization.
	 */
	private void initPortFields() throws ComponentNotReadyException {
		if (inputPort == null) return;
		portReadingIterator = new ReadableChannelPortIterator(inputPort, portProtocolFields.toArray(new String[portProtocolFields.size()]));
		portReadingIterator.setCharset(charset);
		portReadingIterator.setContextURL(contextURL);
		portReadingIterator.setPropertyRefResolver(propertyRefResolve);
		portReadingIterator.init();
	}

	/**
	 * !!!returns!!! 
	 * 		true  - if the source contains data OR if the input port is NOT eof, 
	 * 				then it is necessary to call the method next() that returns null or an input stream. 
	 * 		false - no input data
	 * 
	 * TODO to make hasData method for the InputPort that waits for new data if the edge is empty. Is it good solution???
	 */
	public boolean hasNext() {
		return dictionaryReadingIterator.hasNext() || filenameItor.hasNext() || (bInputPort && portReadingIterator.hasNext());
	}

	/**
	 * @throws JetelException 
	 * @see java.util.Iterator
	 */
	public Object next() throws JetelException {
		// read next value from dictionary array or list
		if (dictionaryReadingIterator.hasNext()) {
			ReadableByteChannel next = dictionaryReadingIterator.next();
			String currentInnerFileName = dictionaryReadingIterator.getCurrentInnerFileName();
			if (currentInnerFileName != null) {
				currentFileName = currentInnerFileName;
			}
			return next;
		}
		
		// read from fields
		if (currentPortProtocolPosition == firstPortProtocolPosition) {
			try {
				Object dataSource = portReadingIterator.getNextData(preferredDataSourceType);
				currentFileName = portReadingIterator.getCurrentFileName();
				return dataSource;
			} catch (NullPointerException e) {
				throw new JetelException("The field '" + portReadingIterator.getLastFieldName() + "' contain unsupported null value.");
			} catch (UnsupportedEncodingException e) {
				throw new JetelException("The field '" + portReadingIterator.getLastFieldName() + "' contain a value that cannot be translated by " + charset + " charset." );
			} catch (Exception e) {
				throw new JetelException("Port reading error", e);
			}
		}
		
		// read from urls
		if (filenameItor.hasNext()) {
			currentFileName = filenameItor.next();
			currentPortProtocolPosition++;
			
			// read from dictionary
			if (currentFileName.indexOf(DICT) == 0) {
				dictionaryReadingIterator.init(currentFileName);
				return next();
			}
			currentFileName = unificateFileName(contextURL, currentFileName);
			
			Object preferredDataSource = getPreferredDataSource(contextURL, currentFileName, preferredDataSourceType);
			if (preferredDataSource != null) {
				return preferredDataSource;
			}
			
			if (preferredDataSourceType == DataSourceType.STREAM) {
				return createInputStream(currentFileName);
			}
			
			return createReadableByteChannel(currentFileName);
		}
		return null;
	}
	
	/**
	 * @return first ReadableByteChannel in the queue of sources (the other types are skipped)
	 * @throws JetelException
	 */
	public ReadableByteChannel nextChannel() throws JetelException {
		Object source = next();
		while (source != null && !(source instanceof ReadableByteChannel)) {
			source = next();
		}
		return (ReadableByteChannel) source;
	}
	
	static String unificateFileName(URL contextURL, String fileName) {
		try {
			// unify only local files
			URI fileURI = getFileURI(contextURL, fileName);
			if (fileURI != null) {
				URL fileURL = fileURI.toURL();
				String sPath = fileURL.getRef() != null ? fileURL.getFile() + "#" + fileURL.getRef() : fileURL.getFile();
				fileName = new File(sPath).getCanonicalFile().toString();
			}
		} catch (Exception e) {
			//NOTHING
		}
		return fileName;
	}
	
	/**
	 * Returns absolute URI with "file" scheme if currentFileName is considered to be a path to a local file.
	 * @param contextURL
	 * @param currentFileName
	 * @return file:/ URI or null 
	 */
	private static URI getFileURI(URL contextURL, String fileName) throws MalformedURLException, URISyntaxException {
		if (fileName.equals(FileUtils.STD_CONSOLE)) return null;

		if (FileURLParser.isServerURL(fileName) || FileURLParser.isArchiveURL(fileName)) return null;
		
		URI fileURI = CloverURI.createSingleURI(contextURL != null ? contextURL.toURI() : null, fileName).toURI();
		return PROTOCOL_FILE.equals(fileURI.getScheme()) ? fileURI : null;
	}
	
	/**
	 * Handles creation of preferred data source form like 'java.io.File' or 'java.net.URI' which are
	 * sometimes needed instead of an anonymous channel.
	 * @return preferred data source or null if no preferred data source specified or it could not be created.
	 * @throws JetelException hmmmf TODO Here is the place where exception with good informative message can be thrown.
	 * If we just fall back to Channel which the parser wouldn't support, the parser has no chance to give a good error message,
	 * only some kind of "Unsupported data source"
	 */
	static Object getPreferredDataSource(URL contextURL, String currentFileName, DataSourceType preferredDataSourceType) throws JetelException {
		if (preferredDataSourceType == DataSourceType.FILE) {
			try {
				File file = FileUtils.getJavaFile(contextURL, currentFileName);
				return file;
			} catch (Exception e) {
				//DO NOTHING - just try to prepare a data source in other way
			}
		}
		
		if (preferredDataSourceType == DataSourceType.URI) {
			try {
				try {
					return CloverURI.createSingleURI(contextURL != null ? contextURL.toURI() : null, currentFileName).getAbsoluteURI().toURI();
				} catch (Exception e) {
					// ignore
				}
				return new URI(currentFileName);
			} catch (URISyntaxException ex) {
				throw new JetelException("Invalid fileURL", ex);
			} catch (Exception e) {
				// DO NOTHING - just try to open a stream based on the currentFileName in the next step
			}
		}
		return null;
	}
	
	/**
	 * If an input port is connected but not used.
	 */
	public void blankRead() {
		// empty read
		if (portReadingIterator != null) portReadingIterator.blankRead();
	}
	
	/**
	 * Creates input stream for a file name.
	 * 
	 * @param fileName
	 * @return
	 * @throws JetelException
	 */
	private InputStream createInputStream(String fileName) throws JetelException {
		defaultLogger.debug("Opening input file " + fileName);
		try {
			InputStream iStream = FileUtils.getInputStream(contextURL, fileName);
			defaultLogger.debug("Reading input file " + fileName);
			return iStream;
		} catch (IOException e) {
			throw new JetelException("File is unreachable: " + fileName, e);
		}
	}
	
	/**
	 * Creates readable channel for a file name.
	 * 
	 * @param fileName
	 * @return
	 * @throws JetelException
	 */
	private ReadableByteChannel createReadableByteChannel(String fileName) throws JetelException {
		defaultLogger.debug("Opening input file " + fileName);
		try {
			ReadableByteChannel channel = FileUtils.getReadableChannel(contextURL, fileName);
			defaultLogger.debug("Reading input file " + fileName);
			return channel;
		} catch (IOException e) {
			throw new JetelException("File is unreachable: " + fileName, e);
		}
	}

	/**
	 * Gets protocol position in file list.
	 * @param files
	 * @param protocol
	 * @return
	 */
	private int getFirstProtocolPosition(List<String> files, String protocol) {
		for (int i=0; i<files.size(); i++) {
			if (files.get(i).indexOf(protocol) == 0) {
			//if (getMostInnerString(files.get(i)).indexOf(protocol) == 0) {
				return i;
			}			
		}
		return -1;
	}
	
	/**
	 * Removes all files that contains the protocol from from the file list.
	 * @param files
	 * @param protocol
	 * @param start
	 * @return
	 */
	private List<String> getAndRemoveProtocol(List<String> files, String protocol, int start) {
		ArrayList<String> result = new ArrayList<String>();
		if (start < 0) return result;
		String file;
		for (int i=start; i<files.size(); i++) {
			file = files.get(i);
			if (file.indexOf(protocol) == 0) {
				files.remove(i);
				i--;
				result.add(file);
			}
		}
		return result;
	}
	
	/**
	 * Gets file iterator.
	 * @return
	 */
	public Iterator<String> getFileIterator() {
		return files.iterator();
	}
	
	/**
	 * Returns current field name.
	 */
	public String getCurrentFileName() {
		return currentFileName;
	}

	public DataRecord getCurrenRecord() {
		
		if(this.portReadingIterator!=null) {
			return this.portReadingIterator.getCurrentRecord();
		}
		return null;
	}
	
	/**
	 * Sets a dictionary for dictionary reading.
	 * @param dictionary
	 */
	public void setDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
	}

	/**
	 * Returns true if file URLs contains port or dictionary file URL.
	 * @return
	 */
	public boolean isGraphDependentSource() {
		return isGraphDependentSource;
	}

	/**
	 * Sets charset.
	 * @param charset
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}

	/**
	 * Sets property resolver.
	 * @param propertyRefResolve
	 */
	public void setPropertyRefResolver(PropertyRefResolver propertyRefResolve) {
		this.propertyRefResolve = propertyRefResolve;
	}

	/**
	 * @param preferredDataSourceType
	 */
	public void setPreferredDataSourceType(DataSourceType preferredDataSourceType) {
		this.preferredDataSourceType = preferredDataSourceType;
	}

}
