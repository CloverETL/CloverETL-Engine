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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.Parser;
import org.jetel.data.parser.Parser.DataSourceType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPort;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.graph.rest.jaxb.RequestParameter;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.HttpPartUrlUtils;
import org.jetel.util.file.stream.Input;
import org.jetel.util.property.PropertyRefResolver;

/**
 * A class for transparent reading of clover data records from multiple input files.
 * The nested parser is used for parsing all input source files.
 * Usage: 
 * - first instantiate some suitable parser, set all its parameters (don't call init method)
 * - optionally set appropriate logger
 * - sets required multifile reader parameters (setFileSkip(), setSkip(), setNumRecords(), ...)
 * - call init method with metadata for reading input sources
 * - at last one can use this reader in the same way as all parsers via nextRecord method called in cycle
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 1.11.2006
 */
public class MultiFileReader {
	
    private static Log defaultLogger = LogFactory.getLog(MultiFileReader.class);
    private static final String UNREACHABLE_FILE = "File is unreachable: ";
    private static final String STD_IN = "-";
    private Log logger = defaultLogger;

    
    private List<MultiFileListener> listeners;
	private Parser parser;
    private URL contextURL;
    private String fileURL;
	private ReadableChannelIterator channelIterator;
    private int skip;
	private int skipSourceRows;
	private int skipL3Rows;
	private int numRecords; //max number of returned records
	private int numSourceRecords;
	private int numL3Records;
    private int skipped; // number of records skipped to satisfy the skip attribute, records skipped to satisfy skipSourceRows and l3Skip are not included
    private int skippedInSource; // number of records skipped to satisfy the skip-per-source attribute, records skipped to satisfy l3Skip are not included
    private boolean noInputFile = false;
    private String incrementalFile;
    private String incrementalKey;
    private IncrementalReading incrementalReading;
    private int iSource;

    private AutoFilling autoFilling = new AutoFilling();
    
    private InputPort inputPort;
	private String charset;
	private Dictionary dictionary;
	private boolean initializeDataDependentSource;
	private boolean isSourceOpen;
	private PropertyRefResolver propertyRefResolve;
    
    /**
	 * Sole ctor.
	 * @param parser Parser to be used to obtain records from input files.
	 * @param fileURL Specification of input file(s)
	 */
	public MultiFileReader(Parser parser, URL contextURL, String fileURL) {
		this.parser = parser;
        this.contextURL = contextURL;
		this.fileURL = fileURL;
		skipL3Rows = 0;
		numL3Records = -1;
	}

    /**
     * Initialization of multi file reader. Calls parser.init() with a given metadata.
     * 
     * @param metadata
     * @throws ComponentNotReadyException
     */
    public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
    	incrementalReading = new IncrementalReading(incrementalFile, incrementalKey);
    	incrementalReading.setContextURL(contextURL);
    	incrementalReading.init();
        parser.init();
    	if (metadata != null) autoFilling.addAutoFillingFields(metadata);
		iSource = -1;
    }

    /**
     * ChannelItereator - check configuration.
     * @throws ComponentNotReadyException
     */
    private void checkChannelIterator() throws ComponentNotReadyException {
    	commonSettingChannelIterator();
    	channelIterator.checkConfig();
    }

    /**
     * ChannelItereator - initialize.
     * @throws ComponentNotReadyException
     */
    private void initChannelIterator() throws ComponentNotReadyException {
    	commonSettingChannelIterator();
    	channelIterator.init();
    }
    
    private void commonSettingChannelIterator() throws ComponentNotReadyException {
    	channelIterator = new ReadableChannelIterator(inputPort, contextURL, fileURL);
    	channelIterator.setCharset(charset);
    	channelIterator.setDictionary(dictionary);
    	channelIterator.setPropertyRefResolver(propertyRefResolve);
		channelIterator.setPreferredDataSourceType(parser.getPreferredDataSourceType());
    }
    
    /**
     * Sets an input port for data reading from input record.
     * 
     * @param metadata
     */
    public void setInputPort(InputPort inputPort) {
    	this.inputPort = inputPort;
    }

    /**
     * @param metadata
     * @throws ComponentNotReadyException
     */
	public void checkConfig(DataRecordMetadata metadata) throws ComponentNotReadyException {
		parser.init();
		
		List<String> requestParameters = new ArrayList<String>();
		if (metadata.getGraph() != null && metadata.getGraph().getEndpointSettings() != null) {
			requestParameters = getRequestParametersNames(metadata.getGraph().getEndpointSettings().getRequestParameters());
		}
		if (requestParameters.isEmpty() || !HttpPartUrlUtils.containsRequestParameter(requestParameters, fileURL)) {
	        try {
	            checkChannelIterator();
	            
	    		String fName = null; 
	    		Iterator<Input> fit = channelIterator.getInputIterator();
	    		boolean closeLastStream = false;
	    		while (fit.hasNext()) {
	    			try {
	    				Input input = fit.next();
	    				fName = input.getAbsolutePath();
	    				Object dataSource = input.getPreferredInput(parser.getPreferredDataSourceType());
	    				if (dataSource == null) {
	    					dataSource = input.getPreferredInput(DataSourceType.CHANNEL);
	    				}
	    				parser.setDataSource(dataSource);
	    				notifyFileChangeListeners(dataSource);
	    				
	    				parser.setReleaseDataSource(closeLastStream = true);
	    			} catch (Exception e) {
	    				throw new ComponentNotReadyException(UNREACHABLE_FILE + fName, e);
	    			}
	    		}
	    		if (closeLastStream) {
	    			try {
	    				parser.close();
	    			} catch (IOException e) {
	    				throw new ComponentNotReadyException("File '" + fName + "' cannot be closed.", e);
	    			}
	    		}
	        } finally {
	        	ReadableChannelIterator.free(channelIterator);
	        }
		}
	}
	
	/**
     * Sets number of skipped records in next call of getNext() method.
     * @param skip
     */
    public void setSkip(int skip) {
    	skipped = 0;
        this.skip = skip;
    }
    
    /**
     * Sets number of skipped records in each file.
     * @param skipSourceRows
     */
    public void setSkipSourceRows(int skipSourceRows) {
        this.skipSourceRows = skipSourceRows;
    }
    
    public void setL3Skip(int l3Skip) {
        this.skipL3Rows = l3Skip;
    }
    
    /**
     * How many rows to process.
     * @param numRecords
     */
    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }
    
    /**
     * How many rows to process for every source.
     * @param numSourceRecords
     */
    public void setNumSourceRecords(int numSourceRecords) {
        this.numSourceRecords = numSourceRecords;
    }
    
    public void setL3NumRecords(int l3NumRecords) {
        this.numL3Records = l3NumRecords;
    }
    
    public void setLogger(Log logger) {
        this.logger = logger;
    }
    
    
    /**
     * Attempts to set next source to underlying data parser.
     * Should be called only if standard MultiFileReader.getNext() can't be used forever reason
     * 
     * @return  true if next data source was successfully set
     * @throws JetelException
     */
    public boolean setNextSource() throws JetelException{
    	return nextSource();
    }
    
    private Object currentSource = null;
    
	/**
     * Switch to the next source file.
	 * @return
	 * @throws JetelException 
	 */
	private boolean nextSource() throws JetelException {
		if (currentSource instanceof Closeable) {
			try {
				// CL-2732: Does not fix the situation when currentSource is a File or a URL.
				((Closeable) currentSource).close();
			} catch (IOException ioe) {
				logger.warn("Failed to close the previous source");
			}
		}

		// update incremental value from previous source
		if (iSource >= 0) incrementalReading.nextSource(channelIterator.getCurrentFileName(), parser.getPosition());
		
		skippedInSource = 0;
		// next source
		Object source = null;
		//TODO close channel
		while (channelIterator.hasNext()) {
			autoFilling.resetSourceCounter();
			autoFilling.resetGlobalSourceCounter();
			autoFilling.resetL3Counter();
			try {
				source = channelIterator.next();
				if (source == null) continue; // if record no record found
				this.currentSource = source; // store the current source, so that we can close it later
				
				String fileName = channelIterator.getCurrentFileName();
				long fileSize = 0;
				Date fileTimestamp = null;
				if (fileName != null && FileUtils.isLocalFile(contextURL, fileName)) {
					// CL-2631 - Use "FileUtils.getJavaFile()" instead of "new File()" to take context URL into account
					File tmpFile = FileUtils.getJavaFile(contextURL, fileName);
					long timestamp = tmpFile.lastModified();
					fileTimestamp = timestamp == 0 ? null : new Date(timestamp);
					fileSize = tmpFile.length();
				}
				autoFilling.setFilename(fileName);
				autoFilling.setFileSize(fileSize);
				autoFilling.setFileTimestamp(fileTimestamp);
				
				iSource++;
				parser.setDataSource(source);
				notifyFileChangeListeners(source);
				Object sourcePosition;
				if ((sourcePosition = incrementalReading.getSourcePosition(channelIterator.getCurrentFileName())) != null) {
					parser.movePosition(sourcePosition);
				}
				skip();
				return isSourceOpen = true;
			} catch (IOException e) {
				throw new JetelException("An error occured while skipping records in file " + autoFilling.getFilename() + ", the file will be ignored", e);
			} catch (ComponentNotReadyException e) {
				throw new JetelException("An error occured while switching input file " + autoFilling.getFilename() + ", the file will be ignored" ,e);
			}
		}
		if (isSourceOpen) {
			try {
				parser.close();
			} catch (IOException e) {
				throw new JetelException("An error occured while closing input file '" + autoFilling.getFilename() + "'.", e);
			}
		}
		return isSourceOpen = false;
	}

	/**
	 * Moves to the next section of a multi-section input source (e.g. for XLS files each spreadsheet is considered a section)
	 */
	private boolean nextL3Source() throws JetelException {
		autoFilling.resetL3Counter();
		if (!parser.nextL3Source()) {
			return false;
		}
		skip();
		return true;
	}

	/**
	 * Performs skip operation at the start of a data source. L3 skip is always performed.
	 * If the target number of locally (per-source) skipped records has not yet been reached, it is followed
	 * by local skip. The same happens later for global skip.
	 * Increases per-source number of records by number of records skipped to satisfy global skip attribute,
	 * L3 number of records is increased by number of records skipped to satisfy local (per-source) and global
	 * skip attribute.
	 * 
	 * @throws JetelException 
	 */
	private void skip() throws JetelException {
    	// perform L3 skip
		if (skipL3Rows > 0) {
			parser.skip(skipL3Rows);
		}
		// perform per-source skip, skipped records are counted by L3 counter
		int numSkippedSource = 0; // number of records skipped in this subsource to satisfy skip-per-source attribute 
		if (skippedInSource < skipSourceRows) {
			// perform per-source skip
			int numSkipSource = skipSourceRows - skippedInSource;
    		if (numL3Records >= 0 && numL3Records < numSkipSource) {
    			// records for global skip in local file are limited by max number of records from local file
    			numSkipSource = numL3Records;
    		}
    		numSkippedSource = parser.skip(numSkipSource);
    		skippedInSource += numSkippedSource;
    		autoFilling.incL3Counter(numSkippedSource);
		}
		// perform global skip, skip records are counted by L3 counter and per-source counter
		if (skipped < skip) {
			int numSkipGlobal = skip - skipped;    			
			if (numL3Records >= 0 && numL3Records - numSkippedSource < numSkipGlobal) {    				
				numSkipGlobal = numL3Records - numSkippedSource;
			}
			if (numSourceRecords >= 0 && numSourceRecords - autoFilling.getSourceCounter() < numSkipGlobal) {
				// records for global skip in local file are limited by max number of records from local file
				numSkipGlobal = Math.min(numSkipGlobal, numSourceRecords - autoFilling.getSourceCounter());
			}
			int numSkippedGlobal = parser.skip(numSkipGlobal);
			skipped += numSkippedGlobal;
			autoFilling.incL3Counter(numSkippedGlobal);
			autoFilling.incSourceCounter(numSkippedGlobal);
		}
    }
	
	    
	/**
	 * Checks skip/numRecords. Returns true if the source could return a record.
	 * @return
	 * @throws JetelException
	 * @throws InterruptedException 
	 */
	private final boolean checkRowAndPrepareSource() throws JetelException, InterruptedException {
        //in case that fileURL doesn't contain valid file url
        initializeDataDependentSource();
        if(noInputFile) {
            return false;
        }
        
        //check for index of last returned record
        if(numRecords > 0 && numRecords <= autoFilling.getGlobalCounter()) {
        	//read remaining input records if any (it is necessary to avoid CLO-5716 and CLO-4577) 
			channelIterator.blankRead(); 
            return false;
        }

        //check for index of last returned record for each source
        if (numSourceRecords > 0 && numSourceRecords <= autoFilling.getSourceCounter()) {
        	if (!nextSource()) {
        		return false;
        	}
        	return checkRowAndPrepareSource();
        }
        if (numL3Records > 0 && numL3Records <= autoFilling.getL3Counter()) {
        	if (!nextL3Source() && !nextSource()) {
        		return false;
        	}
        	return checkRowAndPrepareSource();
        }
        return true;
	}
	
	/**
	 * Creates List of request parameters names.
	 * @param parameters
	 * @return
	 */
	private List<String> getRequestParametersNames(List<RequestParameter> parameters) {
		List<String> result = new ArrayList<String>(parameters.size());
		for (RequestParameter parameter : parameters) {
			result.add(parameter.getName());
		}
		return result;
	}
	
	/**
	 * Tries to obtain one record
	 * @param record Instance to be filled with obtained data
	 * @return null on error, the record otherwise
	 * @throws JetelException
	 * @throws InterruptedException 
	 */
	public DataRecord getNext(DataRecord record) throws JetelException, InterruptedException {
		// checks skip/numRecords
		if (!checkRowAndPrepareSource()) {
			return null;
		}
		
        //use parser to get next record
        DataRecord rec;
        try {
            while ((rec = parser.getNext(record)) == null && (nextL3Source() || nextSource()));
        } catch(JetelException e) {
            autoFilling.incGlobalCounter();
            autoFilling.incSourceCounter();
            autoFilling.incL3Counter();
            throw e;
        } 
        autoFilling.setLastUsedAutoFillingFields(rec);
        
        if (rec == null) channelIterator.blankRead();
        
        return rec;
	}
	
	 public int getNextDirect(CloverBuffer targetBuffer) throws JetelException, InterruptedException {
		 	int success;
		 	// checks skip/numRecords
			if (!checkRowAndPrepareSource()) {
				return 0;
			}
			
	        //use parser to get next record
	        try {
	            try {
	            	do{
	            		success=parser.getNextDirect(targetBuffer);
	            		if (success==-1) break;
	            	}while ((success==0) && (nextL3Source() || nextSource()));
	            } catch(JetelException e) {
	                autoFilling.incGlobalCounter();
	                autoFilling.incSourceCounter();
	                autoFilling.incL3Counter();
	                throw e;
	            } 
	        } catch (RuntimeException ex) {
	        	try {
					throw ex.getClass().getConstructor(RuntimeException.class).newInstance("Error when parsing source: " + channelIterator.getCurrentFileName(), ex);
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
						| InvocationTargetException | NoSuchMethodException | SecurityException e) {
					throw ex;
				}
	        }
	        // no autofilling with direct buffer
	        // autoFilling.setLastUsedAutoFillingFields(rec);
	        autoFilling.incCounters();
	        
	        if (success==0) channelIterator.blankRead();
	        
	        return success;
	 }
	
	public String getSourceName() {
		return channelIterator.getCurrentFileName();
	}

	/**
	 * Tries to obtain one record
	 * @param record Instance to be filled with obtained data
	 * @return null on error, the record otherwise
	 * @throws JetelException
	 * @throws InterruptedException 
	 */
	public DataRecord getNext() throws JetelException, InterruptedException {
		// checks skip/numRecords
		if (!checkRowAndPrepareSource()) {
			return null;
		}
        
        //use parser to get next record
        DataRecord rec;
        try {
            try {
                while((rec = parser.getNext()) == null && nextSource());
            } catch(JetelException e) {
                autoFilling.incGlobalCounter();
                throw e;
            }
        } catch (RuntimeException ex) {
        	try {
				throw ex.getClass().getConstructor(RuntimeException.class).newInstance("Error when parsing source: " + channelIterator.getCurrentFileName(), ex);
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e) {
				throw ex;
			}
        }
        autoFilling.setAutoFillingFields(rec);
        
        if (rec == null) channelIterator.blankRead();
        
        return rec;
	}		

	
	/**
	 * Checks whether wrapped parser supports direct reading to CloverBuffer
	 * 
	 * @return true if direct reading supported
	 */
	public boolean isDirectReadingSupported() {
		return parser.isDirectReadingSupported();
	}
	
	private final void initializeDataDependentSource() throws JetelException {
        if (initializeDataDependentSource) {
        	noInputFile = !nextSource();
        	initializeDataDependentSource = false;
        }
	}
	
	/**
	 * Updates and stores incremental reading values into a file.
	 * @throws IOException 
	 */
	public void storeIncrementalReading() throws IOException {
		incrementalReading.storeIncrementalReading();
	}
	
	/**
	 * @deprecated Replaced by free()
	 * 
	 * Releases resources held by the instance
	 * @throws IOException 
	 *
	 */
	public void close() throws IOException {
		free();
	}
	
	/**
	 * A method to be called by the owner before the start of each phase
	 *  
     * Tries to open the first data source.
	 */
    public void preExecute() throws ComponentNotReadyException {
		initChannelIterator();
		
    	parser.preExecute();
    	
		noInputFile = false;

        try {
    		if(!(initializeDataDependentSource = channelIterator.isGraphDependentSource()) && !nextSource()) { 
    		    noInputFile = true;
    		}
    	} catch (JetelException e) {
            noInputFile = true;
            throw new ComponentNotReadyException(e);
		}
    }
    
    /**
     * A method to be called by the owner after the end of each phase
     */
    public void postExecute() throws ComponentNotReadyException {
		parser.postExecute();
		autoFilling.reset();
		iSource = -1;

		// channelIterator.reset(); // CLO-5399
		skipped = 0;
        try {
    		incrementalReading.reset();
		} catch (IOException e) {
			logger.error("postExecute", e);
		}
        
        ReadableChannelIterator.postExecute(channelIterator);
    }
    
    /*
     * Releases resources held by the instance. It is supposed to be called exactly once by the owner of the instance 
     * @throws IOException 
	 */
    public void free() throws IOException {
    	try {
    		try {
    			ReadableChannelIterator.free(channelIterator);
    		} finally {
    			if (isSourceOpen) {
    				parser.free();
    			}
    		}
    	} catch (Exception e) {
    		logger.error("Failed to release resources orderly", e);
    	}
    }
	
    /**
     * @deprecated Replaced by preExecute(), postExecute()
     * 
	 * Reset reader for next graph execution. 
     * @throws ComponentNotReadyException 
     */
    @Deprecated
	public void reset() throws ComponentNotReadyException {
    	postExecute();
    	preExecute();
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
	}

    public void setIncrementalFile(String incrementalFile) {
    	this.incrementalFile = incrementalFile;
    }

    public void setIncrementalKey(String incrementalKey) {
    	this.incrementalKey = incrementalKey;
    }

    public void setPropertyRefResolver(PropertyRefResolver propertyRefResolve) {
    	this.propertyRefResolve = propertyRefResolve;
    }
    
    /**
     * Adds reference to listener for change in DataSource - invoked
     * after calling Parser.setDataSource();
     * 
     * @param toAdd	class implementing MultiFileListener
     */
    public void addFileChangeListener(MultiFileListener toAdd) {
        if (listeners==null){
        	listeners=new ArrayList<MultiFileListener>();
        }
    	listeners.add(toAdd);
    }
    
    private void notifyFileChangeListeners(Object newFile) {
    	if (listeners!=null){
    		for(MultiFileListener listener: listeners){
    			listener.fileChanged(newFile);
    		}
    	}
    }
    
}
