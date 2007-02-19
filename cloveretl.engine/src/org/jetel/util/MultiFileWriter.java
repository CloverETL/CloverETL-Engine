/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.Formatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Class for transparent writing into multifile or multistream. Underlying formatter is used for formatting
 * incoming data records and destination is a list of files defined in fileURL attribute
 * by org.jetel.util.MultiOutFile or iterator of writable channels.
 * Usage: 
 * - first instantiate some suitable formatter, set all its parameters (don't call init method)
 * - optionally set appropriate logger
 * - sets required multifile writer parameters (setAppendData(), setRecordsPerFile(), setBytesPerLine(), ...)
 * - call init method with metadata for reading input sources
 * - at last one can use this writer in the same way as all formatter via write method called in cycle

 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 2.11.2006
 */
public class MultiFileWriter {

    private static Log defaultLogger = LogFactory.getLog(MultiFileWriter.class);
    private Log logger = defaultLogger;

    private Formatter formatter;
    private URL contextURL;
    private String fileURL;
    private String charset;
    private int recordsPerFile;
    private int bytesPerFile;
    private int records;
    private int bytes;
    private boolean appendData;
    private ByteBuffer header;
    private ByteBuffer footer;
    private Iterator<String> fileNames;
    private Iterator<WritableByteChannel> channels;
    private WritableByteChannel byteChannel;
    private int skip;
	private int numRecords;
	private int counter;
    
    /**
     * Constructor.
     * @param formatter formatter is used for incoming records formatting
     * @param fileURL target file(s) definition
     */
    public MultiFileWriter(Formatter formatter, URL contextURL, String fileURL) {
        this.formatter = formatter;
        this.contextURL = contextURL;
        this.fileURL = fileURL;
    }

    public MultiFileWriter(Formatter formatter, Iterator<WritableByteChannel> channels) {
        this.formatter = formatter;
        this.channels = channels;
    }

    /**
     * Initializes underlying formatter with a given metadata.
     * @param metadata
     * @throws ComponentNotReadyException 
     */
    public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
    	if (fileURL != null) fileNames = new MultiOutFile(fileURL);
        formatter.init(metadata);
        try {
            setNextOutput();
        } catch(IOException e) {
            throw new ComponentNotReadyException(e);
        }
    }
    
    /**
     * Switch output file for formatter.
     * @throws IOException 
     * @throws FileNotFoundException
     */
    private void setNextOutput() throws IOException {
    	if (fileNames != null && !fileNames.hasNext()) {
            logger.warn("Unable to open new output file. This may be caused by missing wildcard in filename specification. "
                    + "Size of output file will exceed specified limit.");
            return;
    	}
    	if (channels != null && !channels.hasNext()) {
            logger.warn("Unable to open new output stream. Size of last output stream will exceed specified limit.");
            return;
        }      	

        //write footer to the previous destination if it is not first call of this method
        if(byteChannel != null) {
            writeFooter();
        }
        byteChannel = fileNames != null ? FileUtils.getWritableChannel(contextURL, fileNames.next(), appendData) : channels.next();
        //write header
        writeHeader();
        formatter.setDataTarget(byteChannel);
    }

    private void writeHeader() throws IOException {
        if(header != null) {
            byteChannel.write(header);
            header.rewind();
        }
    }
    
    private void writeFooter() throws IOException {
        if(footer != null) {
            byteChannel.write(footer);
            footer.rewind();
        }
    }
    
    /**
     * Writes given record via formatter into destination file(s).
     * @param record
     * @throws IOException
     */
    public void write(DataRecord record) throws IOException {
        //check for index of last returned record
        if(numRecords > 0 && numRecords == counter) {
            return;
        }
        
        //shall i skip some records?
        if(skip > 0) {
            skip--;
            return;
        }
    	
    	if ((recordsPerFile > 0 && records >= recordsPerFile)
                || (bytesPerFile > 0 && bytes >= bytesPerFile)) {
            setNextOutput();
            records = 0;
            bytes = 0;
        }
        bytes += formatter.write(record);
        records++;
        counter++;
    }

    /**
     * Closes underlying formatter.
     */
    public void close() {
        formatter.close();
    }
    
    /**
     * Sets number of bytes written into separate file.
     * @param bytesPerFile
     */
    public void setBytesPerFile(int bytesPerFile) {
        this.bytesPerFile = bytesPerFile;
    }

    /**
     * Sets number of records written into seprate file.
     * @param recordsPerFile
     */
    public void setRecordsPerFile(int recordsPerFile) {
        this.recordsPerFile = recordsPerFile;
    }
    
    public void setLogger(Log logger) {
        this.logger = logger;
    }

    public void setAppendData(boolean appendData) {
        this.appendData = appendData;
    }

    public void setFooter(String footer) {
        if(charset != null) {
            try {
                this.footer = ByteBuffer.wrap(footer.getBytes(charset));
                return;
            } catch (UnsupportedEncodingException e) {
                logger.warn(e);
                charset = null;
            }
        }
        this.footer = ByteBuffer.wrap(footer.getBytes());
    }

    public void setHeader(String header) {
        if(charset != null) {
            try {
                this.header = ByteBuffer.wrap(header.getBytes(charset));
                return;
            } catch (UnsupportedEncodingException e) {
                logger.warn(e);
                charset = null;
            }
        }
        this.header = ByteBuffer.wrap(header.getBytes());
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    /**
     * Sets number of skipped records in next call of getNext() method.
     * @param skip
     */
    public void setSkip(int skip) {
        this.skip = skip;
    }

    /**
     * Sets number of read reacords
     * @param numRecords
     */
    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }

}
