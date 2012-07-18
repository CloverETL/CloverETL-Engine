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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.JetelVersion;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.BitArray;

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
public class CloverDataParser extends AbstractParser {

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
	
	private final static int LONG_SIZE_BYTES = 8;
    private final static int LEN_SIZE_SPECIFIER = 4;

    public CloverDataParser(DataRecordMetadata metadata){
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

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	@Override
	public void setReleaseDataSource(boolean releaseInputSource)  {
	}

    /* (non-Javadoc)
     * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
     * 
     * parameter: data fiele name or {data file name, index file name}
     */
    @Override
	public void setDataSource(Object in) throws ComponentNotReadyException {
    	sourceRecordCounter = 0;
    	currentIndexPosition = 0;
    	indexFile = null;
    	
        if (in instanceof String[]) {
        	inData = ((String[])in)[0];
        	indexFileURL = ((String[])in)[1];
        }else if (in instanceof String){
        	inData = (String)in;
        	indexFileURL = null;
        } else if (in instanceof InputStream) {
        	inStream = (InputStream) in;
        	indexFileURL = null;
        }
        
        if (inData != null) {
            try{
            	String fileName = new File(FileUtils.getFile(projectURL, inData)).getName();
            	if (fileName.toLowerCase().endsWith(".zip")) {
            		fileName = fileName.substring(0,fileName.lastIndexOf('.')); 
            	}
                recordFile = FileUtils.getReadableChannel(projectURL, !inData.startsWith("zip:") ? inData : 
                	inData + "#" + CloverDataFormatter.DATA_DIRECTORY + fileName);
                	
                //read and check header of clover binary data format to check out the compatibility issues
                checkCompatibilityHeader(recordFile);
                initIndexFile(fileName);
            } catch (IOException ex) {
                throw new ComponentNotReadyException(ex);
            }
        } else if (inStream != null) {
        	indexFile = null;
        	recordFile = Channels.newChannel(inStream);
        	//read and check header of clover binary data format to check out the compatibility issues
            checkCompatibilityHeader(recordFile);
        }
    	recordBuffer.clear();
		try {
			ByteBufferUtils.reload(recordBuffer.buf(),recordFile);
			recordBuffer.flip();
		} catch (IOException e) {
			throw new ComponentNotReadyException(e.getLocalizedMessage());
		}
    }

    public static void checkCompatibilityHeader(ReadableByteChannel recordFile) throws ComponentNotReadyException {
        ByteBuffer headerBuffer = ByteBuffer.allocateDirect(Defaults.Component.CLOVER_DATA_HEADER_SIZE);
        headerBuffer.clear();
        
    	try {
			if (recordFile.read(headerBuffer) != Defaults.Component.CLOVER_DATA_HEADER_SIZE) {
	        	//clover binary data format is definitely incompatible with current version - header is not present
	        	throw new ComponentNotReadyException("Source clover data file is obsolete. " +
	        			"Data cannot be read. Header data structure is missing or invalid.");
			}
		} catch (IOException e) {
        	throw new ComponentNotReadyException(e);
		}
    	headerBuffer.flip();
    	
        //read clover binary data header and check backward compatibility
        //better header description is at CloverDataFormatter.setDataTarget() method
        if (Defaults.Component.CLOVER_DATA_HEADER != headerBuffer.getLong()) {
        	//clover binary data format is definitely incompatible with current version - header is not present
        	throw new ComponentNotReadyException("Source clover data file is obsolete. Data cannot be read.");
        }
        if (Defaults.Component.CLOVER_DATA_COMPATIBILITY_HASH != headerBuffer.getLong()) {
        	byte majorVersion = headerBuffer.get();
        	byte minorVersion = headerBuffer.get();
        	byte revisionVersion = headerBuffer.get();
        	//clover binary data format is incompatible with current version - compatibility hash was changed
        	throw new ComponentNotReadyException("Source clover data file is obsolete (version " + majorVersion + "." + minorVersion + "." + revisionVersion + "). Data cannot be read.");
        }
    	byte majorVersion = headerBuffer.get();
    	byte minorVersion = headerBuffer.get();
    	byte revisionVersion = headerBuffer.get();
    	if (majorVersion != JetelVersion.getMajorVersion() || minorVersion != JetelVersion.getMinorVersion()) {
    		logger.warn("Source clover data file was produced by incompatible clover engine version " + majorVersion + "." + minorVersion + "." + revisionVersion + ". It is not encouraged usage of clover binary format.");
    	}
    	byte[] extraBytes = new byte[4];
    	headerBuffer.get(extraBytes);
    	if (BitArray.isSet(extraBytes, 0) ^ Defaults.Record.USE_FIELDS_NULL_INDICATORS) {
        	throw new ComponentNotReadyException("Source file with binary data format is not compatible. Engine producer has different setup of Defaults.Record.USE_FIELDS_NULL_INDICATORS (see documentation). Data cannot be read.");
    	}
    }

    private boolean initIndexFile(String fileName) throws ComponentNotReadyException {
    	indexFile = null;
    	try {
	        if (inData.startsWith("zip:")){
	            indexFile = new DataInputStream(FileUtils.getInputStream(projectURL, inData + "#" + CloverDataFormatter.INDEX_DIRECTORY + 
	            		fileName + CloverDataFormatter.INDEX_EXTENSION));
	        }else{//read index from binary file
	            if (indexFileURL == null){
	            	indexFile = new DataInputStream(FileUtils.getInputStream(projectURL, inData + CloverDataFormatter.INDEX_EXTENSION));
	            }else{
	            	indexFile = new DataInputStream((FileUtils.getInputStream(projectURL, indexFileURL)));
	            }
	        }
    	} catch (IOException e) {
    		indexFile = null;
    	}
        return indexFile != null;
    }
    
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#close()
	 */
	@Override
	public void close() {
		if (recordFile != null) {
			try {
				if (recordFile.isOpen()) {
					recordFile.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		if (indexFile != null) {
			try {
				indexFile.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getNext(org.jetel.data.DataRecord)
	 */
	@Override
	public DataRecord getNext(DataRecord record)throws JetelException{
		// the skip rows has skipped whole file
		if (noDataAvailable) return null;
		
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
			return null;
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
				throw new JetelException(ex.getLocalizedMessage());
			}
			recordBuffer.flip();
		}
		record.deserialize(recordBuffer);
		sourceRecordCounter++;
		return record;
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
    }
    
	@Override
    public void free() {
    	close();
    }

	@Override
	public boolean nextL3Source() {
		return false;
	}
	
}
