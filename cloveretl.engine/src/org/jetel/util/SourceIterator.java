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
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.data.parser.Parser.DataSourceType;
import org.jetel.enums.ProcessingType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPort;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.WcardPattern;
import org.jetel.util.property.PropertyRefResolver;

//TODO: this is work in progress!
/**
 * FYI this class is based on {@link ReadableChannelIterator} and can be full featured substitution in the future
 * 
 *  UPDATE: ReadableChannelIterator has meanwhile evolved. It now handles namely preferred data source types {@link DataSourceType}. 
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 19.10.2011
 */
public class SourceIterator {
	
	private static Log DEFAULT_LOGGER = LogFactory.getLog(SourceIterator.class);

	public static final String DICT_PREFIX = "dict:";
	public static final String DICT_DISCRETE_SUFFIX = ":" + ProcessingType.DISCRETE;
	private static final String PORT_PREFIX = "port:";
	private static final String FILE_PROTOCOL_PREFIX = "file";

	private static final String DEFAULT_CHARSET = "UTF-8";
	private static final String UNKNOWN_SOURCE_NAME = "unknown";

	private String fileURL;
	private URL contextURL;
	private Iterator<String> filenameIterator;
	private List<String> files;

	private InputPort inputPort;
	private ReadableChannelPortIterator portReadingIterator;
	private PropertyRefResolver propertyRefResolve; // for port reading - source mode

	private Dictionary dictionary;
	private ReadableChannelDictionaryIterator dictionaryIterator;
	private String charset;

	private int firstPortProtocolPosition;
	private List<String> portProtocolFields;

	private int currentSourcePosition = 0;
	private String currentSourceName = UNKNOWN_SOURCE_NAME;
	
	private ProcessingType defaultProcessingType = ProcessingType.STREAM;
	
	// true if fileURL contains port or dictionary protocol
	private boolean isGraphDependentSource;

	public SourceIterator(InputPort inputPort, URL contextURL, String fileURL) {
		this.inputPort = inputPort;
		this.fileURL = fileURL;
		this.contextURL = contextURL;
	}

	public void checkConfig(ConfigurationStatus status) throws ComponentNotReadyException {
		// TODO: implement properly
		init();
	}

	public void init() throws ComponentNotReadyException {
		if (charset == null) {
			charset = DEFAULT_CHARSET;
		}

		// FIXME: resolving all files at this point is premature as files might not exist yet
		prepareSourceIterator();
		preparePortIterator();
		prepareDictionaryIterator();
	}

	public void preExecute() throws ComponentNotReadyException {
		// do nothing
	}

	/**
	 * returns true if the source contains data OR if the input port is NOT eof, then it is necessary to call the method
	 * next() that returns null or an input stream. false - no input data
	 * 
	 */
	public boolean hasNext() {
		return filenameIterator.hasNext() || dictionaryIterator.hasNext() ||
			   (portReadingIterator != null && portReadingIterator.hasNext());
	}

	/**
	 * @throws JetelException
	 * @see java.util.Iterator
	 */
	public Object next() throws JetelException {
		// read next value from dictionary array or list
		if (dictionaryIterator.hasNext()) {
			return dictionaryIterator.next();
		}

		// read from fields
		if (currentSourcePosition == firstPortProtocolPosition) {
			try {
				Object dataSource = portReadingIterator.getNextData(DataSourceType.CHANNEL);
				currentSourceName = portReadingIterator.getCurrentFileName();
				return dataSource;
			} catch (NullPointerException e) {
				throw new JetelException("The field '" + portReadingIterator.getLastFieldName() + "' contain unsupported null value.");
			} catch (UnsupportedEncodingException e) {
				throw new JetelException("The field '" + portReadingIterator.getLastFieldName() + "' contain an value that cannot be translated by " + charset + " charset.");
			} catch (Exception e) {
				throw new JetelException("Port reading error", e);
			}
		}

		// read from urls
		if (filenameIterator.hasNext()) {
			currentSourceName = filenameIterator.next();
			currentSourcePosition++;

			// read from dictionary
			if (currentSourceName.indexOf(DICT_PREFIX) == 0) {
				SourceWithProcessingType sourceWithProcessingType = resolveProcessingType(currentSourceName.substring(DICT_PREFIX.length()));
				if (sourceWithProcessingType.processingType == ProcessingType.DISCRETE) {
					return dictionary.getValue(sourceWithProcessingType.source);
				}
				dictionaryIterator.init(currentSourceName);
				return next();
			}
			currentSourceName = unificateFileName(currentSourceName);
			return createReadableByteChannel(currentSourceName);
		}
		return null;
	}
	
	private SourceWithProcessingType resolveProcessingType(String source) {
		for (ProcessingType procType : ProcessingType.values()) {
			String procTypeName = procType.getId();
			if (source.endsWith(":" + procTypeName)) {
				return new SourceWithProcessingType(source.substring(0, source.length() - procTypeName.length()), procType);
			}
		}
		return new SourceWithProcessingType(source, defaultProcessingType);
	}
	
	private static class SourceWithProcessingType {
		public final String source;
		public final ProcessingType processingType;
		
		public SourceWithProcessingType(String source, ProcessingType processingType) {
			this.source = source;
			this.processingType = processingType;
		}
	}

	public void postExecute() throws ComponentNotReadyException {
		currentSourcePosition = 0;
		currentSourceName = UNKNOWN_SOURCE_NAME;
	}

	/**
	 * File iterator initialization.
	 * 
	 * @throws ComponentNotReadyException
	 */
	private void prepareSourceIterator() throws ComponentNotReadyException {
		WcardPattern pat = new WcardPattern();
		pat.setParent(contextURL);
		pat.addPattern(fileURL, Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
		pat.resolveAllNames(true);
		try {
			files = pat.filenames();
		} catch (IOException e) {
			throw new ComponentNotReadyException("Can't prepare file list from wildcard URL", e);
		}

		firstPortProtocolPosition = getFirstProtocolPosition(files, PORT_PREFIX);
		if (firstPortProtocolPosition >= 0 && inputPort == null)
			throw new ComponentNotReadyException("Input port is not defined for '" + files.get(firstPortProtocolPosition) + "'.");

		portProtocolFields = getAndRemoveProtocol(files, PORT_PREFIX, firstPortProtocolPosition);
		this.filenameIterator = files.iterator();
		
		isGraphDependentSource = firstPortProtocolPosition == 0 || getFirstProtocolPosition(files, DICT_PREFIX) == 0;
	}

	private void preparePortIterator() throws ComponentNotReadyException {
		if (inputPort == null) {
			return;
		}

		portReadingIterator = new ReadableChannelPortIterator(inputPort, portProtocolFields.toArray(new String[portProtocolFields.size()]));
		portReadingIterator.setCharset(charset);
		portReadingIterator.setContextURL(contextURL);
		portReadingIterator.setPropertyRefResolver(propertyRefResolve);
		portReadingIterator.init();
	}

	private void prepareDictionaryIterator() {
		dictionaryIterator = new ReadableChannelDictionaryIterator(dictionary);
		dictionaryIterator.setCharset(charset);
	}

	private String unificateFileName(String fileName) {
		try {
			// standard console -> do nothing
			if (currentSourceName.equals(FileUtils.STD_CONSOLE))
				return currentSourceName;

			// remote file -> do nothing
			if (FileURLParser.isServerURL(fileName) || FileURLParser.isArchiveURL(fileName))
				return currentSourceName;

			// unify only local files
			URL fileURL = FileUtils.getFileURL(contextURL, currentSourceName);
			if (fileURL.getProtocol().equals(FILE_PROTOCOL_PREFIX)) {
				String sPath = fileURL.getRef() != null ? fileURL.getFile() + "#" + fileURL.getRef() : fileURL.getFile();
				currentSourceName = new File(sPath).getCanonicalFile().toString();
			}
		} catch (Exception e) {
			// NOTHING
		}
		return currentSourceName;
	}

	/**
	 * If an input port is connected but not used.
	 */
	public void blankRead() {
		// empty read
		if (portReadingIterator != null) {
			portReadingIterator.blankRead();
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
		DEFAULT_LOGGER.debug("Opening input file " + fileName);
		try {
			ReadableByteChannel channel = FileUtils.getReadableChannel(contextURL, fileName);
			DEFAULT_LOGGER.debug("Reading input file " + fileName);
			return channel;
		} catch (IOException e) {
			throw new JetelException("File is unreachable: " + fileName, e);
		}
	}

	/**
	 * Gets protocol position in file list.
	 * 
	 * @param files
	 * @param protocol
	 * @return
	 */
	private int getFirstProtocolPosition(List<String> files, String protocol) {
		for (int i = 0; i < files.size(); i++) {
			if (files.get(i).indexOf(protocol) == 0) {
				// if (getMostInnerString(files.get(i)).indexOf(protocol) == 0) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * Removes all files that contains the protocol from from the file list.
	 * 
	 * @param files
	 * @param protocol
	 * @param start
	 * @return
	 */
	private List<String> getAndRemoveProtocol(List<String> files, String protocol, int start) {
		ArrayList<String> result = new ArrayList<String>();
		if (start < 0)
			return result;
		String file;
		for (int i = start; i < files.size(); i++) {
			file = files.get(i);
			if (file.indexOf(protocol) == 0) {
				files.remove(i);
				i--;
				result.add(file);
			}
		}
		return result;
	}

	public Iterator<String> getFileIterator() {
		return files.iterator();
	}
	
	public boolean isSingleSource() {
		return files.size() <= 1;
	}

	public String getCurrentFileName() {
		return currentSourceName;
	}

	public void setDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setPropertyRefResolver(PropertyRefResolver propertyRefResolve) {
		this.propertyRefResolve = propertyRefResolve;
	}

	public ProcessingType getDefaultProcessingType() {
		return defaultProcessingType;
	}

	/**
	 * Sets default processing type for port/dictionary reading. Used when no processing type specified in input URL.
	 * Port/dictionary reading URL syntax: port:$0.FieldName[:processingType]
	 * 
	 * Default default processing type is "stream".
	 * 
	 * @param defaultProcessingType
	 */
	public void setDefaultProcessingType(ProcessingType defaultProcessingType) {
		this.defaultProcessingType = defaultProcessingType;
	}

	public boolean isGraphDependentSource() {
		return isGraphDependentSource;
	}
}
