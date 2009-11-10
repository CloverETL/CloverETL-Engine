/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPort;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
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

	private Parser parser;
    private URL contextURL;
    private String fileURL;
	private ReadableChannelIterator channelIterator;
    private int skip;
    private int fSkip;
	private int skipSourceRows;
	private int numRecords; //max number of returned records
	private int numSourceRecords;
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
	}

    /**
     * Initialization of multi file reader. Calls parser.init() with a given metadata.
     * Tries open first data source.
     * @param metadata
     * @throws ComponentNotReadyException
     */
    public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
    	incrementalReading = new IncrementalReading(incrementalFile, incrementalKey);
    	incrementalReading.setContextURL(contextURL);
    	incrementalReading.init();
        parser.init(metadata);
    	if (metadata != null) autoFilling.addAutoFillingFields(metadata);
		iSource = -1;
        
		initChannelIterator();
        try {
            if(!(initializeDataDependentSource = channelIterator.isGraphDependentSource()) && !nextSource()) {
                noInputFile = true;
                //throw new ComponentNotReadyException("FileURL attribute (" + fileURL + ") doesn't contain valid file url.");
            }
        } catch (JetelException e) {
            noInputFile = true;
            throw new ComponentNotReadyException(e.getMessage()/*"FileURL attribute (" + fileURL + ") doesn't contain valid file url."*/, e);
        }
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
        parser.init(metadata);
        checkChannelIterator();
        
		String fName = null; 
		Iterator<String> fit = channelIterator.getFileIterator();
		boolean closeLastStream = false;
		while (fit.hasNext()) {
			try {
				fName = fit.next();
				if (fName.equals(STD_IN)) continue;
				if (fName.startsWith("dict:")) continue; //this test has to be here, since an involuntary warning is caused
				String mostInnerFile = FileURLParser.getMostInnerAddress(fName);
				URL url = FileUtils.getFileURL(contextURL, mostInnerFile);
				if (FileUtils.isServerURL(url)) {
					//FileUtils.checkServer(url); //this is very long operation
					continue;
				}
				if (FileURLParser.isArchiveURL(fName)) {
					// test if the archive file exists
					// getReadableChannel is too long for archives
					if (new File(url.getFile()).exists()) continue;
					throw new ComponentNotReadyException(UNREACHABLE_FILE + fName);
				}
				parser.setDataSource(FileUtils.getReadableChannel(contextURL, fName));
				parser.setReleaseDataSource(closeLastStream = true);
			} catch (IOException e) {
				throw new ComponentNotReadyException(UNREACHABLE_FILE + fName, e);
			} catch (ComponentNotReadyException e) {
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
	}
	
	/**
     * Sets number of skipped records in next call of getNext() method.
     * @param skip
     */
    public void setSkip(int skip) {
        this.skip = fSkip = skip;
    }
    
    /**
     * Sets number of skipped records in each file.
     * @param skipSourceRows
     */
    public void setSkipSourceRows(int skipSourceRows) {
        this.skipSourceRows = skipSourceRows;
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
    
    public void setLogger(Log logger) {
        this.logger = logger;
    }
    
	/**
     * Switch to the next source file.
	 * @return
	 * @throws JetelException 
	 */
	private boolean nextSource() throws JetelException {
		// update incremental value from previous source
		if (iSource >= 0) incrementalReading.nextSource(channelIterator.getCurrentFileName(), parser.getPosition());
		
		// next source
		ReadableByteChannel stream = null;
		//TODO close channel
		while (channelIterator.hasNext()) {
			autoFilling.resetSourceCounter();
			autoFilling.resetGlobalSourceCounter();
			try {
				stream = channelIterator.next();
				if (stream == null) continue; // if record no record found
				autoFilling.setFilename(channelIterator.getCurrentFileName());
				File tmpFile = new File(autoFilling.getFilename());
				long timestamp = tmpFile.lastModified();
				autoFilling.setFileSize(tmpFile.length());
				autoFilling.setFileTimestamp(timestamp == 0 ? null : new Date(timestamp));				
				iSource++;
				parser.setDataSource(stream);
				parser.setReleaseDataSource(!autoFilling.getFilename().equals(STD_IN));
				Object sourcePosition;
				if ((sourcePosition = incrementalReading.getSourcePosition(channelIterator.getCurrentFileName())) != null) {
					parser.movePosition(sourcePosition);
				}
				if(skipSourceRows > 0) parser.skip(skipSourceRows);
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
	 * This private method try to skip records given in <code>skip</code> variable.
     * @param skip number of skipped records
	 * @throws JetelException 
	 */
	private void skip(int skip) throws JetelException {
        int skipped = 0;

        do {
            try {
                skipped += parser.skip(skip - skipped);            
            } catch (JetelException e) {
                logger.error("An error occured while skipping records in file " + autoFilling.getFilename() + ", the file will be ignored", e);
            }
        } while (skipped < skip && nextSource());
    }
    
	/**
	 * Checks skip/numRecords. Returns true if the source could return a record.
	 * @return
	 * @throws JetelException
	 */
	private final boolean checkRowAndPrepareSource() throws JetelException {
        //in case that fileURL doesn't contain valid file url
        initializeDataDependentSource();
        if(noInputFile) {
            return false;
        }
        
        //check for index of last returned record
        if(numRecords > 0 && numRecords == autoFilling.getGlobalCounter()) {
            return numSourceRecords > 0 && nextSource();
        }
        
        //check for index of last returned record for each source
        if(numSourceRecords > 0 && numSourceRecords == autoFilling.getSourceCounter()) {
            return numRecords > autoFilling.getGlobalCounter() || nextSource();
        }
        
        //shall i skip some records?
        if(skip > 0) {
            skip(skip);
            skip = 0;
        }
        return true;        
	}
	
	/**
	 * Tries to obtain one record
	 * @param record Instance to be filled with obtained data
	 * @return null on error, the record otherwise
	 * @throws JetelException
	 */
	public DataRecord getNext(DataRecord record) throws JetelException {
		// checks skip/numRecords
		if (!checkRowAndPrepareSource()) {
			return null;
		}
		
        //use parser to get next record
        DataRecord rec;
        try {
            while((rec = parser.getNext(record)) == null && nextSource());
        } catch(JetelException e) {
            autoFilling.incGlobalCounter();
            autoFilling.incSourceCounter();
            throw e;
        }
        autoFilling.setLastUsedAutoFillingFields(rec);
        
        if (rec == null) channelIterator.blankRead();
        
        return rec;
	}		

	/**
	 * Tries to obtain one record
	 * @param record Instance to be filled with obtained data
	 * @return null on error, the record otherwise
	 * @throws JetelException
	 */
	public DataRecord getNext() throws JetelException {
		// checks skip/numRecords
		if (!checkRowAndPrepareSource()) {
			return null;
		}
        
        //use parser to get next record
        DataRecord rec;
        try {
            while((rec = parser.getNext()) == null && nextSource());
        } catch(JetelException e) {
            autoFilling.incGlobalCounter();
            throw e;
        }
        autoFilling.setAutoFillingFields(rec);
        
        if (rec == null) channelIterator.blankRead();
        
        return rec;
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
	 * Releases resources held by the instance
	 * @throws IOException 
	 *
	 */
	public void close() throws IOException {
		if (isSourceOpen) 
			parser.close();
	}
	
    /**
	 * Reset reader for next graph execution. 
     * @throws ComponentNotReadyException 
     */
	public void reset() throws ComponentNotReadyException {
		parser.reset();
		noInputFile = false;
		autoFilling.reset();
		iSource = -1;
		skip = fSkip;

		channelIterator.reset();
        try {
    		incrementalReading.reset();
			if(!(initializeDataDependentSource = channelIterator.isGraphDependentSource()) && !nextSource()) 
			    noInputFile = true;
		} catch (JetelException e) {
			logger.error("reset", e);
		} catch (IOException e) {
			logger.error("reset", e);
		}
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
}
