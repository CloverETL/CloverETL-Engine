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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.CloverDataRecordSerializer;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.DataRecordSerializer;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.data.formatter.CloverDataFormatter.DataCompressAlgorithm;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.graph.ContextProvider;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.BitArray;
import org.jetel.util.stream.CloverDataStream;

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
public class CloverDataParser extends AbstractParser implements ICloverDataParser {

	
	public static class FileConfig {
    	public byte majorVersion;
    	public byte minorVersion;
    	public byte revisionVersion;
    	public int compressionAlgorithm;
    	public CloverDataFormatter.DataFormatVersion formatVersion;
    	public DataRecordMetadata metadata;
    	public boolean raw;
	}
	
	
	
	private final static Log logger = LogFactory.getLog(CloverDataParser.class);

	private DataRecordMetadata metadata;
	private ReadableByteChannel recordFile;
	private CloverDataStream.Input input;
	private CloverBuffer recordBuffer;
	private InputStream inStream;
	private URL projectURL;
	
    private DataRecordSerializer serializer;
    
    private CloverDataFormatter.DataCompressAlgorithm compress;
    
       
	/** Clover version which has been used to create the input data file. */
	private FileConfig version;

    /** In case the input file has been created by clover 3.4 and current job type is jobflow
     * special de-serialisation needs to be used, see CLO-1382 */
	private boolean useParsingFromJobflow_3_4 = false;

	/**
	 * True, if the current transformation is jobflow.
	 */
	private boolean isJobflow;

	private final static int LONG_SIZE_BYTES = 8;
    private final static int LEN_SIZE_SPECIFIER = 4;

    public CloverDataParser(DataRecordMetadata metadata){
    	this.metadata = metadata;
    	this.compress = DataCompressAlgorithm.NONE;
    }
    
    
	public FileConfig getVersion() {
		return version;
	}


	public InputStream getInStream() {
		return inStream;
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
		if (isDirectReadingSupported()) {
			CloverBuffer buffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
			for (int skipped = 0; skipped < nRec; skipped++) {
				if (getNextDirect(buffer)) {
					return skipped;
				}
			}
		} else {
			DataRecord record = DataRecordFactory.newRecord(metadata);
			record.init();
			for (int skipped = 0; skipped < nRec; skipped++) {
				if (getNext(record) == null) {
					return skipped;
				}
			}
		}
		return nRec;
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
        recordBuffer.order(CloverDataFormatter.BUFFER_BYTE_ORDER);
        serializer=new CloverDataRecordSerializer();
	}

	private void doReleaseDataSource() throws IOException {
		if (inStream != null) {
			inStream.close();
			inStream = null; // setDataSource() tests inStream for null
		}
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
    	String inData=null;
    	if (releaseDataSource) {
    		releaseDataSource();
    	}
    	
        if (in instanceof InputStream) {
        	inStream = (InputStream) in;
        }else if (in instanceof ReadableByteChannel){
        	inStream = Channels.newInputStream((ReadableByteChannel)in);
        }else if (in instanceof File){
        	try {
				inStream = new FileInputStream((File)in);
			} catch (IOException e) {
				throw new ComponentNotReadyException(e);
			}
        }else if (in instanceof String[]) {
        	inData = ((String[])in)[0];
        }else if (in instanceof String){
        	inData = (String)in;
    	}else{
        	throw new ComponentNotReadyException("Unsupported Data Source type: "+in.getClass().getName());
        }
        
        if (inStream==null) { // doReleaseDataSource() should set the previous stream to null
        		try{
                String fileName = new File(FileUtils.getFile(projectURL, inData)).getName();
                 if (fileName.toLowerCase().endsWith(".zip")) {
                 		fileName = fileName.substring(0,fileName.lastIndexOf('.')); 
                 }
                     inStream = FileUtils.getInputStream(projectURL, !inData.startsWith("zip:") ? inData : 
                     	inData + "#" + CloverDataFormatter.DATA_DIRECTORY + fileName);
                     	
                 } catch (IOException ex) {
                     throw new ComponentNotReadyException(ex);
                 }
        }
        
		//read and check header of clover binary data format to check out the compatibility issues
	     version = checkCompatibilityHeader(inStream, metadata);
	     if(version.formatVersion!=CloverDataFormatter.CURRENT_FORMAT_VERSION){
	    	 return;
	     }
	     
		 this.compress=DataCompressAlgorithm.getAlgorithm(version.compressionAlgorithm);
        
        //is the current transformation jobflow?
        isJobflow = ContextProvider.getRuntimeContext() != null
        		&& ContextProvider.getRuntimeContext().getJobType().isJobflow();
        
        //in case the input file has been created by clover 3.4 or 3.3 and current job type is jobflow
        //special de-serialisation needs to be used, see CLO-1382
        if (version.majorVersion == 3 
        		&& (version.minorVersion == 3 || version.minorVersion == 4)
        		&& isJobflow) {
        	useParsingFromJobflow_3_4 = true;
        }
        switch(compress){
        case NONE:
        	this.input= new CloverDataStream.Input(inStream);
        	break;
        case LZ4:
        	this.input= new CloverDataStream.Input(inStream, new CloverDataStream.DecompressorLZ4());
        	break;
        case GZIP:
        	this.input= new CloverDataStream.Input(inStream, new CloverDataStream.DecompressorGZIP());
        	break;
        	default:
        		throw new RuntimeException("Unsupported compression algorithm: "+compress);
        }
        
    }

    public static FileConfig checkCompatibilityHeader(ReadableByteChannel recordFile, DataRecordMetadata metadata) throws ComponentNotReadyException {
    	return checkCompatibilityHeader(Channels.newInputStream(recordFile),metadata);
    }
    
    public static FileConfig checkCompatibilityHeader(InputStream recordFile, DataRecordMetadata metadata) throws ComponentNotReadyException {
    	byte[] extraBytes;
    	CloverBuffer buffer = CloverBuffer.wrap(new byte[CloverDataFormatter.CLOVER_DATA_HEADER_LENGTH]);
		try {
			int count = recordFile.read(buffer.array());
			if (count != buffer.capacity()) {
				throw new IOException("Failed to read file header");
			}
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
		 //read clover binary data header and check backward compatibility
        //better header description is at CloverDataFormatter.setDataTarget() method
		long cloverHash=buffer.getLong();
        if (CloverDataFormatter.CLOVER_DATA_HEADER != cloverHash) {
        	//clover binary data format is definitely incompatible with current version - header is not present
        	throw new ComponentNotReadyException("Source clover data file is obsolete. Data cannot be read.");
        }
        long cloverDataCompatibilityHash = buffer.getLong();
        FileConfig version = new FileConfig();
    	version.majorVersion = buffer.get();
    	version.minorVersion = buffer.get();
    	version.revisionVersion = buffer.get();
    	if (cloverDataCompatibilityHash == CloverDataFormatter.CLOVER_DATA_COMPATIBILITY_HASH_2_9){
    		version.formatVersion=CloverDataFormatter.DataFormatVersion.VERSION_29;
    	}else if (cloverDataCompatibilityHash == CloverDataFormatter.CLOVER_DATA_COMPATIBILITY_HASH_3_5){
    		version.formatVersion=CloverDataFormatter.DataFormatVersion.VERSION_35;
    	}else if (cloverDataCompatibilityHash == CloverDataFormatter.CLOVER_DATA_COMPATIBILITY_HASH_4_0){
    		version.formatVersion=CloverDataFormatter.DataFormatVersion.VERSION_40;
    	}else{
    		throw new ComponentNotReadyException("Invallid Clover Data Compatibility Hash: "+cloverDataCompatibilityHash);
    	}
    	
    	switch(version.formatVersion){
    	case VERSION_29:
    	case VERSION_35:
    		extraBytes = new byte[CloverDataFormatter.HEADER_OPTIONS_ARRAY_SIZE_3_5];
    		try {
    			recordFile.read(extraBytes);
    		} catch (IOException e) {
    			throw new ComponentNotReadyException(e);
    		}
	    	if (BitArray.isSet(extraBytes, 0) ^ Defaults.Record.USE_FIELDS_NULL_INDICATORS) {
	        	throw new ComponentNotReadyException("Source file with binary data format is not compatible. Engine producer has different setup of Defaults.Record.USE_FIELDS_NULL_INDICATORS (see documentation). Data cannot be read.");
	    	}
	    	// what's left is metadata serialized, will let this to "other" parser 
    		break;
    	case VERSION_40:
    		extraBytes = new byte[CloverDataFormatter.HEADER_OPTIONS_ARRAY_SIZE];
    		try {
    			int count = recordFile.read(extraBytes);
    			if (count != extraBytes.length) {
    				throw new IOException("Failed to read file header");
    			}
    		} catch (IOException e) {
    			throw new ComponentNotReadyException(e);
    		}
        	if (BitArray.isSet(extraBytes, 0) ^ Defaults.Record.USE_FIELDS_NULL_INDICATORS) {
            	throw new ComponentNotReadyException("Source file with binary data format is not compatible. Engine producer has different setup of Defaults.Record.USE_FIELDS_NULL_INDICATORS (see documentation). Data cannot be read.");
        	}
        	version.compressionAlgorithm=BitArray.extractNumber(extraBytes, CloverDataFormatter.OPTION_MASK_COMPRESSED_DATA);
        	version.raw = BitArray.extractNumber(extraBytes, CloverDataFormatter.OPTION_MASK_RAW_DATA) == 1;
        	//check metadata (just read,do not control now)
        	int metasize;
        	try {
    			metasize=ByteBufferUtils.decodeLength(recordFile);
    			byte[] metadef=new byte[metasize];
    			if (recordFile.read(metadef)!=metasize){ 
    				throw new IOException("Not enough data in file.");
    			}
    	    	version.metadata=DataRecordMetadataXMLReaderWriter.readMetadata(new ByteArrayInputStream(metadef));
    		} catch (IOException e) {
    			throw new ComponentNotReadyException("Unable to read metadata definition from CloverData file", e);
    		}
        	
    		break;
    		default:
    			throw new ComponentNotReadyException("Source clover data file is not supported (version " + version.majorVersion + "." + version.minorVersion + "." + version.revisionVersion + "). Data cannot be read.");
    	}
    		
    	return version;
    }

 
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#close()
	 */
	@Override
	public void close() {
		releaseDataSource();
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getNext(org.jetel.data.DataRecord)
	 */
	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		if (!getNextDirect(recordBuffer)) {
			return null; //end of file reached
		}
		
		if (!useParsingFromJobflow_3_4) {
			record.deserializeUnitary(recordBuffer,serializer);
		} else {
			record.deserialize(recordBuffer,serializer);
		}
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
	public boolean getNextDirect(CloverBuffer targetBuffer) throws JetelException {
		final int size;
		try {
			size=ByteBufferUtils.decodeLength(input);
			if (size<0) return false; //end of file reached
		
			targetBuffer.clear();
			targetBuffer.limit(size);
			if (input.read(targetBuffer)==-1){
				throw new JetelException("Insufficient data in datastream.");
			}
			
			targetBuffer.flip();
		} catch(IOException ex){
			throw new JetelException(ex);
		}
		
		return true;
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
	public boolean isDirectReadingSupported() {
		return getVersion().raw;
	}
	
}
