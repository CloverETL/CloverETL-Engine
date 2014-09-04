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
package org.jetel.graph;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.jetel.data.DataRecord;
import org.jetel.data.parser.BinaryDataParser;
import org.jetel.data.parser.BinaryDataParser.NoDataAvailableException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.MetadataUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.file.FileUtils;

/**
 * This class reads data records written by {@link EdgeDebugWriter}.
 * {@link EdgeDebugWriter} and {@link EdgeDebugReader} classes are
 * used to store requested data records flowing through an edge to temporary files.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 11. 2013
 */
public class EdgeDebugReader {

    /** Name of input file */
    private final String debugFile;

    /** Metadata of read data records. */
    private DataRecordMetadata metadata;

    /** Instance of parser, which is used for data records serialisation. */
    private BinaryDataParser parser;

    /** Data records source */
    private ReadableByteChannel inputChannel;
    
    public EdgeDebugReader(String debugFile) {
        this.debugFile = debugFile;
    }
    
    public EdgeDebugReader(InputStream inputStream) {
    	inputChannel = Channels.newChannel(inputStream);
    	this.debugFile = null;
    }
    
    public void init() throws ComponentNotReadyException, IOException, InterruptedException {
    	if (inputChannel == null) {
    		inputChannel = FileUtils.getReadableChannel(ContextProvider.getContextURL(), debugFile);
    	}
    	
    	try {
    		metadata = MetadataUtils.readMetadata(inputChannel);
    	} catch (Exception e) {
    		throw new JetelRuntimeException("Metadata extraction from debug file '" + debugFile + "' failed.", e);
    	}
    	
    	parser = new BinaryDataParser(metadata);
    	parser.setUnitaryDeserialization(true);
    	parser.init();
    	parser.setDataSource(inputChannel);
    }

    /**
	 * Reads previously stored debug record into the given record reference.
	 * 
	 * @param record
	 *            the record that will be filled with data
	 * 
	 * @return the (1-based) ordinal of the data record<br/>
	 *         <code>-1</code> if there are currently no more records<br/>
	 *         <code>-2</code> if there are no more records and will not be (writing EdgeDebuger closed)
	 * 
	 * @throws IOException
	 *             if any I/O error occurs
	 * @throws InterruptedException
	 */
    public int readRecord(DataRecord record) throws IOException, InterruptedException {
    	int ordinal;
    	
    	try {
    		ordinal = parser.getNextInt();
    	} catch(NoDataAvailableException e) {
    		return -1;
    	}
    	if (ordinal != -1) {
    		if (parser.getNext(record) != null) {
    			return ordinal;
    		} else {
    			throw new JetelRuntimeException("Unexpected end of edge debug file.");
    		}
		} else {
			return -2;
		}
	}

    /**
	 * Reads previously stored debug record into the given {@link CloverBuffer}.
	 * 
	 * @param record
	 *            the CloverBuffer that will be filled with data
	 * 
	 * @return the (1-based) ordinal of the data record<br/>
	 *         <code>-1</code> if there are currently no more records<br/>
	 *         <code>-2</code> if there are no more records and will not be (writing EdgeDebuger closed)
	 * 
	 * @throws IOException
	 *             if any I/O error occurs
	 * @throws InterruptedException
	 */
    public int readRecord(CloverBuffer record) throws IOException, InterruptedException {
    	int ordinal;
    	
    	try {
    		ordinal = parser.getNextInt();
    	} catch(NoDataAvailableException e) {
    		return -1;
    	}
    	if (ordinal != -1) {
    		if (parser.getNext(record)) {
    			return ordinal;
    		} else {
    			throw new JetelRuntimeException("Unexpected end of edge debug file.");
    		}
		} else {
			return -2;
		}
	}

	/**
	 * Closes the EdgeDebuger (if buffer used, it writes it to the tape). In writing mode it also writes end flag (
	 * <code>-1</code>) to the tape to indicate that all data has been written (equivalent of EOF). View Data
	 * "Load more" functionality relies on this.
	 */
	public void close() {
		parser.free();
	}

	/**
	 * @return metadata of read data records
	 */
	public DataRecordMetadata getMetadata() {
		return metadata;
	}
}
