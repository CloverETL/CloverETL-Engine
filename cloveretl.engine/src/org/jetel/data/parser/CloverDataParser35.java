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
package org.jetel.data.parser;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.Token;
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.JobType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.file.FileUtils;

/**
 * Class for reading data saved in Clover internal format
 * It is predicted that zip file (with name dataFile.zip) has following structure:
 * DATA/dataFile
 * INDEX/dataFile.idx
 * If data are not in zip file, indexes (if needed) have to be in the same location
 * 
 * <p><b>NOTE:</b>Supports also deserialization of {@link DataRecord}s from an input stream.
 * In such scenario it does not support index file. Generally the storage level should be
 * more generic (like other parsers) so that this class would not depend on specific data sources.</p> 
 * 
 * @author avackova <agata.vackova@javlinconsulting.cz> ;
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 13, 2006
 *
 */
public class CloverDataParser35 extends AbstractParser implements ICloverDataParser {

	private final static Log logger = LogFactory.getLog(CloverDataParser.class);

	private DataRecordMetadata metadata;
	private ReadableByteChannel recordFile;
	private CloverBuffer recordBuffer;
	private String indexFileURL;
	private String inData;
	private InputStream inStream;
	private URL projectURL;
	
	private boolean noDataAvailable;
    private DataInputStream indexFile;
    private long currentIndexPosition;
    private long sourceRecordCounter;
	
    /** In case the input file has been created by clover 3.4 and current job type is jobflow
     * special de-serialisation needs to be used, see CLO-1382 */
	private boolean useParsingFromJobflow_3_4 = false;

	/**
	 * True, if the current transformation is jobflow.
	 */
	private boolean isJobflow;

	private final static int LONG_SIZE_BYTES = 8;
    private final static int LEN_SIZE_SPECIFIER = 4;
    private CloverDataParser.FileConfig version;
    
    
    public CloverDataParser35(DataRecordMetadata metadata){
    	this.metadata = metadata;
    }
    
    
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getNext()
	 */
	@Override
	public DataRecord getNext() throws JetelException {
		DataRecord record = DataRecordFactory.newRecord(metadata);
		record.init();
		return getNext(record);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#skip(int)
	 */
	@Override
	public int skip(int nRec) throws JetelException {
		if (nRec == 0) {
			return 0;
		}
		if (indexFile != null) {
			long currentDataPosition;
			long nextDataPosition;
			long skipBytes = sourceRecordCounter*LONG_SIZE_BYTES - currentIndexPosition;
			long indexSkippedBytes = 0;
			try {
				// find out what is the current index in the input stream
				if (skipBytes != indexFile.skip(skipBytes)) {
					throw new JetelException("Unable to skip in index file - it seems to be corrupt");					
				}
				currentIndexPosition += skipBytes;
				try {
					currentDataPosition = indexFile.readLong();
					currentIndexPosition += LONG_SIZE_BYTES;
				} catch (EOFException e) {
					throw new JetelException("Unable to find index for current record - index file seems to be corrupt");					
				}				
				// find out what is the index of the record following skipped records
				skipBytes = (nRec - 1)*LONG_SIZE_BYTES;
				indexSkippedBytes = indexFile.skip(skipBytes);
				currentIndexPosition += indexSkippedBytes;
				nextDataPosition = currentDataPosition;
				try {
					nextDataPosition = indexFile.readLong();
					currentIndexPosition += LONG_SIZE_BYTES;
				} catch (EOFException e) {
					noDataAvailable = true;
				}				
			} catch (IOException e) {
				throw new JetelException("An IO error occured while trying to skip data in index file", e);
			}
			long dataSkipBytes = nextDataPosition - currentDataPosition;
			try {
				while (dataSkipBytes > recordBuffer.remaining()) {
					dataSkipBytes -= recordBuffer.remaining();
					recordBuffer.clear();
					ByteBufferUtils.reload(recordBuffer.buf(),recordFile);
					recordBuffer.flip();
					if (!recordBuffer.hasRemaining()) { // no more data available
						break;
					}
				}
				if (dataSkipBytes > recordBuffer.remaining()) { // there are not enough data available in the record file
					throw new JetelException("Index file inconsistent with record file");
				}
				recordBuffer.position(recordBuffer.position() + (int)dataSkipBytes);
			} catch (IOException e) {
				throw new JetelException("An IO error occured while trying to skip data in record file", e);				
			}
			return (int)indexSkippedBytes%LONG_SIZE_BYTES;
		} else {
			DataRecord record = DataRecordFactory.newRecord(metadata);
			record.init();
			for (int skipped = 0; skipped < nRec; skipped++) {
				if (getNext(record) == null) {
					return skipped;
				}
			}
			return nRec;
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#init(org.jetel.metadata.DataRecordMetadata)
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
        recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
	}

	private void doReleaseDataSource() throws IOException {
		FileUtils.closeAll(recordFile, indexFile);
	}
	
	@Override
	protected void releaseDataSource() {
		try {
			doReleaseDataSource();
		} catch (IOException ioe) {
			logger.warn("Failed to release data source", ioe);
		}
	}


    /* (non-Javadoc)
     * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
     * 
     * parameter: data fiele name or {data file name, index file name}
     */
    @Override
	public void setDataSource(Object in) throws ComponentNotReadyException {
    	if (releaseDataSource) {
    		releaseDataSource();
    	}
    	sourceRecordCounter = 0;
    	currentIndexPosition = 0;
    	indexFile = null;
    	
    	if (in instanceof InputStream) {
        	inStream = (InputStream) in;
        	indexFileURL = null;
        	recordFile = Channels.newChannel(inStream);
        } else if (in instanceof ReadableByteChannel) {
        	recordFile = (ReadableByteChannel) in;
        	indexFileURL = null;
        	inStream = Channels.newInputStream(recordFile);
        }
        noDataAvailable=false;
    	
    	recordBuffer.clear();
		try {
			ByteBufferUtils.reload(recordBuffer.buf(),recordFile);
			recordBuffer.flip();
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
        //read and check header of clover binary data format to check out the compatibility issues
        readStoredMetadata(recordBuffer, metadata);
    	
    	//is the current transformation jobflow?
        isJobflow = ContextProvider.getRuntimeContext() != null
        		&& ContextProvider.getRuntimeContext().getJobType() == JobType.JOBFLOW;
        
        //in case the input file has been created by clover 3.4 or 3.3 and current job type is jobflow
        //special de-serialisation needs to be used, see CLO-1382
        if (version.majorVersion == 3 
        		&& (version.minorVersion == 3 || version.minorVersion == 4)
        		&& isJobflow) {
        	useParsingFromJobflow_3_4 = true;
        }
    }

	private void readStoredMetadata(CloverBuffer buffer, DataRecordMetadata metadata)
			throws ComponentNotReadyException {
		if (getVersion().formatVersion == CloverDataFormatter.DataFormatVersion.VERSION_35) {
			// check metadata compatibility - 
			DataRecordMetadata persistedMetadata = DataRecordMetadata.deserialize(buffer);
			if (!metadata.equals(persistedMetadata, false)) {
				logger.error("Data structure of input file is not compatible with used metadata. File data structure: " + persistedMetadata.toStringDataTypes());
				throw new ComponentNotReadyException("Data structure of input file is not compatible with used metadata. More details available in log.");
			}
		}
	}

   
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#close()
	 */
	@Override
	public void close() {
		releaseDataSource();
	}
	
	/**
	 * Makes sure there is at least one complete record
	 * in <code>recordBuffer</code>.
	 * Also sets the position of <code>recordBuffer</code> to the beginning
	 * of the record.
	 * <p>
	 * Returns the length of the next serialized record in bytes.
	 * </p>
	 *  
	 * @return length of the next serialized record in <code>recordBuffer</code> in bytes
	 * or -1 if no data is available
	 * @throws JetelException
	 */
	private int fillRecordBuffer() throws JetelException {
		// the skip rows has skipped whole file
		if (noDataAvailable) return -1;
		
		//refill buffer if we are on the end of buffer
		if (recordBuffer.remaining() < LEN_SIZE_SPECIFIER) {
			try {
				ByteBufferUtils.reload(recordBuffer.buf(),recordFile);
				recordBuffer.flip();
			} catch (IOException e) {
				throw new JetelException(e.getLocalizedMessage());
			}
		}
		if (recordBuffer.remaining() < LEN_SIZE_SPECIFIER){
			noDataAvailable = true;
			return -1;
		}
		int recordSize = recordBuffer.getInt();
		//refill buffer if we are on the end of buffer
		if (recordBuffer.remaining() < recordSize) {
			recordBuffer.compact();
			
			if (recordBuffer.capacity() < recordSize) {
				recordBuffer.expand(0, recordSize);
			}
			try {
				recordFile.read(recordBuffer.buf());
			} catch(IOException ex) {
				throw new JetelException(ex.getMessage(),ex);
			}
			recordBuffer.flip();
		}
		
		return recordSize;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getNext(org.jetel.data.DataRecord)
	 */
	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		if (fillRecordBuffer() < 0) {
			return null;
		}
		
		if (!useParsingFromJobflow_3_4) {
			record.deserializeUnitary(recordBuffer);
		} else {
			record.deserialize(recordBuffer);
		}
		
		sourceRecordCounter++;
		return record;
	}
	
	/**
	 * Reads the next serialized record into the provided buffer.
	 * The target buffer is cleared first.
	 * <p>
	 * The position of the target buffer will be set to 0
	 * and the limit will be set to the end of the serialized record.
	 * </p><p>
	 * Returns the provided buffer or <code>null</code> 
	 * if there is no record available.
	 * </p>
	 * 
	 * @param targetBuffer the target buffer
	 * @return <code>targetBuffer</code> or <code>null</code> if no data available
	 * @throws JetelException
	 */
	@Override
	public int getNextDirect(CloverBuffer targetBuffer) throws JetelException {
		int recordSize = fillRecordBuffer();
		if (recordSize < 0) {
			return 0;
		}
		
	    targetBuffer.clear();
	    
		//in case current transformation is jobflow, tokenId must be added to targetBuffer
		//since tokenId is not part of clover data file
		//this is not applied for data files created by 3.3 and 3.4 clover versions,
		//where tokenId has been serialised into clover data files
		if (isJobflow && !useParsingFromJobflow_3_4) {
			Token.serializeTokenId(-1, targetBuffer);
		}

		int oldLimit = recordBuffer.limit(); // store old limit
		recordBuffer.limit(recordBuffer.position() + recordSize); // set new limit
		targetBuffer.put(recordBuffer); // copy data up to new limit and update recordBuffer position
		recordBuffer.limit(oldLimit); // restore old limit
		targetBuffer.flip(); // prepare for reading
		
		sourceRecordCounter++;
		return 1;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setExceptionHandler(org.jetel.exception.IParserExceptionHandler)
	 */
	@Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getExceptionHandler()
	 */
	@Override
	public IParserExceptionHandler getExceptionHandler() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getPolicyType()
	 */
	@Override
	public PolicyType getPolicyType() {
		return null;
	}

	@Override
	public void reset() {
		close();
	}

	@Override
	public Object getPosition() {
		return null;
	}

	@Override
	public void movePosition(Object position) {
	}

	public URL getProjectURL() {
		return projectURL;
	}

	public void setProjectURL(URL projectURL) {
		this.projectURL = projectURL;
	}

	@Override
    public void preExecute() throws ComponentNotReadyException {
    	reset();
    }
    
	@Override
    public void postExecute() throws ComponentNotReadyException {
		if (releaseDataSource) {
			releaseDataSource();
		}
    }
    
	@Override
    public void free() {
    	close();
    }

	@Override
	public boolean nextL3Source() {
		return false;
	}


	@Override
	public CloverDataParser.FileConfig getVersion() {
		return version;
	}


	public void setVersion(CloverDataParser.FileConfig version) {
		this.version = version;
	}

	@Override
	public boolean isDirectReadingSupported() {
		return true;
	}
	
}
