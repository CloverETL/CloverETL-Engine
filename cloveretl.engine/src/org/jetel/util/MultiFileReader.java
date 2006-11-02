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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * A class for transparent reading of clover data records from multiple input files.
 * The nested parser is used for parsing all input source files.
 * Usage: 
 * - first instantiate some suitable parser, set all its parameters (don't call open or init method)
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
    private Log logger = defaultLogger;

	private Parser parser;
    private String fileURL;
	private Iterator<String> filenameItor;
    private int skip;
	private int fileSkip;
	private int counter; //number of returned records
	private int numRecords; //max number of returned records
    private String filename;
    private boolean noInputFile = false;
	/**
	 * Sole ctor.
	 * @param parser Parser to be used to obtain records from input files.
	 * @param fileURL Specification of input file(s)
	 * @param skip Number of records to be skipped
	 * @param fileSkip Number of records to be skipped in each input file
	 * @param maxRecCnt Max number of records to read
	 * @param fileMaxRecCnt Max number of records to read from each file
	 */
	public MultiFileReader(Parser parser, String fileURL) {
		this.parser = parser;
		this.fileURL = fileURL;
	}

    /**
     * Initialization of multi file reader. Calls parser.init() with a given metadata.
     * Tries open first data source.
     * @param metadata
     * @throws ComponentNotReadyException
     */
    public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
        parser.open(null, metadata);
        
        WcardPattern pat = new WcardPattern();
        pat.addPattern(fileURL, Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
        this.filenameItor = pat.filenames().iterator();
        if(!nextSource()) {
            logger.warn("FileURL attribute (" + fileURL + ") doesn't contain valid file url.");
            noInputFile = true;
        }
    }

    /**
     * Sets number of skipped records in next call of getNext() method.
     * @param skip
     */
    public void setSkip(int skip) {
        this.skip = skip;
    }
    
    /**
     * Sets number of skipped records in each file.
     * @param fileSkip
     */
    public void setFileSkip(int fileSkip) {
        this.fileSkip = fileSkip;
    }
    
    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }
    
    public void setLogger(Log logger) {
        this.logger = logger;
    }
    
	/**
     * Switch to the next source file.
	 * @return
	 */
	private boolean nextSource() {
		ReadableByteChannel stream = null; 
		while (filenameItor.hasNext()) {
			filename = filenameItor.next();
			logger.debug("Opening input file " + filename);
			try {
				stream = FileUtils.getReadableChannel(filename);
				logger.debug("Reading input file " + filename);
				parser.setDataSource(stream);
				if(fileSkip > 0) parser.skip(fileSkip);
				return true;
			} catch (IOException e) {
				logger.error("Skipping unreadable file " + filename, e);
				continue;
			} catch (JetelException e) {
				logger.error("An error occured while skipping records in file " + filename + ", the file will be ignored", e);
				continue;
			}
		}
		return false;
	}

	/**
	 * This private method try to skip records given in <code>skip</code> variable.
     * @param skip number of skipped records
	 */
	private void skip(int skip) {
        int skipped = 0;

        do {
            try {
                skipped += parser.skip(skip - skipped);            
            } catch (JetelException e) {
                logger.error("An error occured while skipping records in file " + filename + ", the file will be ignored", e);
            }
        } while (skipped < skip && nextSource());
    }
    
	/**
	 * Tries to obtain one record
	 * @param record Instance to be filled with obtained data
	 * @return null on error, the record otherwise
	 * @throws JetelException
	 */
	public DataRecord getNext(DataRecord record) throws JetelException {
        //in case that fileURL doesn't contain valid file url
        if(noInputFile) {
            return null;
        }
        
        //check for index of last returned record
        if(numRecords > 0 && numRecords == counter) {
            return null;
        }
        
        //shall i skip some records?
        if(skip > 0) {
            skip(skip);
            skip = 0;
        }
        
        //use parser to get next record
        DataRecord rec;
        try {
            while((rec = parser.getNext(record)) == null && nextSource());
        } catch(JetelException e) {
            counter++;
            throw e;
        }
        if(rec != null) {
            counter++;
        }
        
        return rec;
	}		

	/**
	 * Releases resources held by the instance
	 *
	 */
	public void close() {
		parser.close();
	}
}
