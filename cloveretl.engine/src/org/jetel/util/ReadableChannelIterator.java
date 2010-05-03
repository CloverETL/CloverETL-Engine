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
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPort;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.WcardPattern;
import org.jetel.util.property.PropertyRefResolver;

/***
 * Supports a field/dictionary reading and reading from urls.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) OpenSys (www.opensys.eu)
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
		common(false);
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
			throw new ComponentNotReadyException(e);
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
		return filenameItor.hasNext() || (bInputPort && portReadingIterator.hasNext());
	}

	/**
	 * @throws JetelException 
	 * @see java.util.Iterator
	 */
	public ReadableByteChannel next() throws JetelException {
		// read next value from dictionary array or list
		if (dictionaryReadingIterator.hasNext()) {
			return dictionaryReadingIterator.next();
		}
		
		// read from fields
		if (currentPortProtocolPosition == firstPortProtocolPosition) {
			try {
				ReadableByteChannel channel = portReadingIterator.getNextData();
				currentFileName = portReadingIterator.getCurrentFileName();
				return channel;
			} catch (NullPointerException e) {
				throw new JetelException("The field '" + portReadingIterator.getLastFieldName() + "' contain unsupported null value.");
			} catch (UnsupportedEncodingException e) {
				throw new JetelException("The field '" + portReadingIterator.getLastFieldName() + "' contain an value that cannot be translated by " + charset + " charset." );
			} catch (Exception e) {
				throw new JetelException("Port reading error: " + e.getMessage(), e);
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
			currentFileName = unificateFileName(currentFileName);
			return createReadableByteChannel(currentFileName);
		}
		return null;
	}

	private String unificateFileName(String fileName) {
		try {
			if (currentFileName.equals(FileUtils.STD_CONSOLE)) return currentFileName;
			URL fileURL = FileUtils.getFileURL(contextURL, currentFileName);
			if (fileURL.getProtocol().equals(PROTOCOL_FILE)) {
				currentFileName = new File(fileURL.getFile()).getCanonicalFile().toString();
			}
		} catch (Exception e) {
			//NOTHING
		}
		return currentFileName;
	}
	
	/**
	 * If an input port is connected but not used.
	 */
	public void blankRead() {
		// empty read
		if (portReadingIterator != null) portReadingIterator.blankRead();
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
	
}
