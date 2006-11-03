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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.Formatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Class for transparent writing into multifile. Underlying formatter is used for formatting
 * incoming data records and destination is a list of files defined in fileURL attribute 
 * by org.jetel.util.MultiOutFile.
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
    private String fileURL;
    private int recordsPerFile;
    private int bytesPerFile;
    private int records;
    private int bytes;
    private Iterator<String> fileNames;
    private boolean appendData;
    
    /**
     * Constructor.
     * @param formatter formatter is used for incoming records formatting
     * @param fileURL target file(s) definition
     */
    public MultiFileWriter(Formatter formatter, String fileURL) {
        this.formatter = formatter;
        this.fileURL = fileURL;
    }
    
    /**
     * Initializes underlying formatter with a given metadata.
     * @param metadata
     * @throws ComponentNotReadyException 
     */
    public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
        fileNames = new MultiOutFile(fileURL);
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
        if (!fileNames.hasNext()) {
            logger.warn("Unable to open new output file. This may be caused by missing wildcard in filename specification. "
                    + "Size of output file will exceed specified limit");
            return;
        }
        
        formatter.setDataTarget(FileUtils.getWritableChannel(fileNames.next(), appendData));
    }

    /**
     * Writes given record via formatter into destination file(s).
     * @param record
     * @throws IOException
     */
    public void write(DataRecord record) throws IOException {
        if ((recordsPerFile > 0 && records >= recordsPerFile)
                || (bytesPerFile > 0 && bytes >= bytesPerFile)) {
            setNextOutput();
            records = 0;
            bytes = 0;
        }
        bytes += formatter.write(record);
        records++;
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

}
