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

import java.io.BufferedInputStream;
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
import org.jetel.data.CompressingDataRecordSerializer;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.DataRecordSerializer;
import org.jetel.data.Defaults;
import org.jetel.data.Token;
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.data.formatter.CloverDataFormatter.DataCompressAlgorithm;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.graph.ContextProvider;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.metadata.MetadataUtils;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.BitArray;
import org.jetel.util.stream.CloverDataStream;
import org.jetel.util.stream.StreamUtils;

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

	protected DataRecordMetadata metadata;
	private CloverDataStream.Input input;
	private CloverBuffer recordBuffer;
	private InputStream inStream;
	private URL projectURL;
	
    private DataRecordSerializer serializer;
    
    private CloverDataFormatter.DataCompressAlgorithm compress;
    
       
	/** Clover version which has been used to create the input data file. */
	private FileConfig version;

	/**
	 * True, if the current transformation is jobflow.
	 */
	private boolean isJobflow;

    public CloverDataParser(DataRecordMetadata metadata){
    	this.metadata = metadata;
    	this.compress = DataCompressAlgorithm.NONE;
    }
    
    
	@Override
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
		if (version.raw) {
			CloverBuffer buffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
			for (int skipped = 0; skipped < nRec; skipped++) {
				if (getNextDirect(buffer)!=1) {
					return skipped;
				}
			}
		} else {
			DataRecord record = DataRecordFactory.newRecord(metadata);
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
    		// doReleaseDataSource() should set the previous stream to null
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
			try {
				inStream = FileUtils.getInputStream(projectURL, inData);
			} catch (IOException ex) {
				throw new ComponentNotReadyException(ex);
			}
    	}else{
        	throw new ComponentNotReadyException("Unsupported Data Source type: "+in.getClass().getName());
        }
        
		//read and check header of clover binary data format to check out the compatibility issues
	     version = checkCompatibilityHeader(inStream, metadata);
	     if(version.formatVersion!=CloverDataFormatter.CURRENT_FORMAT_VERSION){
	    	 return;
	     }
	     
	    this.serializer = version.raw ? new CloverDataRecordSerializer() : new CompressingDataRecordSerializer();
	     
		 this.compress=DataCompressAlgorithm.getAlgorithm(version.compressionAlgorithm);
        
        //is the current transformation jobflow?
        isJobflow = ContextProvider.getRuntimeContext() != null
        		&& ContextProvider.getRuntimeContext().getJobType().isJobflow();
        
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
    
    /**
     * Reads file header from the provided stream, including metadata. 
     * It is assumed the stream will be closed afterwards.
     * 
     * @param recordFile
     * @return file header with metadata
     * @throws IOException
     */
    public static FileConfig readHeader(InputStream recordFile) throws IOException {
    	try {
    		BufferedInputStream bis = new BufferedInputStream(recordFile);
			FileConfig header = checkCompatibilityHeader(bis, null);
			if (header.formatVersion == CloverDataFormatter.DataFormatVersion.VERSION_35) {
				DataRecordMetadata metadata = DataRecordMetadata.deserialize(bis);
				metadata.setParsingType(DataRecordParsingType.DELIMITED);
				metadata.setRecordDelimiter("\r\n");
				metadata.setFieldDelimiter("|");
				header.metadata = metadata;
			}
			return header;
		} catch (ComponentNotReadyException e) {
			if (e.getCause() instanceof IOException) {
				throw (IOException) e.getCause();
			} else {
				throw new IOException(e.getMessage(), e.getCause());
			}
		}
    }
    
    public static FileConfig checkCompatibilityHeader(InputStream recordFile, DataRecordMetadata metadata) throws ComponentNotReadyException {
    	byte[] extraBytes;
    	CloverBuffer buffer = CloverBuffer.wrap(new byte[CloverDataFormatter.CLOVER_DATA_HEADER_LENGTH]);
		try {
			int count = StreamUtils.readBlocking(recordFile, buffer.array());
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
    		version.raw=true;
    		extraBytes = new byte[CloverDataFormatter.HEADER_OPTIONS_ARRAY_SIZE_3_5];
    		try {
    			int count = StreamUtils.readBlocking(recordFile, extraBytes);
    			if (count != extraBytes.length) {
    				throw new IOException("Failed to read file header");
    			}
    		} catch (IOException e) {
    			throw new ComponentNotReadyException(e);
    		}
	    	// what's left is metadata serialized, will let this to "other" parser 
    		break;
    	case VERSION_40:
    		extraBytes = new byte[CloverDataFormatter.HEADER_OPTIONS_ARRAY_SIZE];
    		try {
    			int count = StreamUtils.readBlocking(recordFile, extraBytes);
    			if (count != extraBytes.length) {
    				throw new IOException("Failed to read file header");
    			}
    		} catch (IOException e) {
    			throw new ComponentNotReadyException(e);
    		}
        	version.compressionAlgorithm=BitArray.extractNumber(extraBytes, CloverDataFormatter.OPTION_MASK_COMPRESSED_DATA);
        	version.raw = BitArray.extractNumber(extraBytes, CloverDataFormatter.OPTION_MASK_RAW_DATA) == 1;
        	//check metadata (just read,do not control now)
        	int metasize;
        	try {
    			metasize=ByteBufferUtils.decodeLength(recordFile);
    			if (metasize < 0) {
    				// CLO-5868: error reporting improved
    				throw new IOException("Unexpected end of data stream");
    			}
    			byte[] metadef=new byte[metasize];
    			if (StreamUtils.readBlocking(recordFile, metadef) != metasize){ 
    				throw new IOException("Not enough data in file.");
    			}
    	    	version.metadata=DataRecordMetadataXMLReaderWriter.readMetadata(new ByteArrayInputStream(metadef));
    		} catch (IOException e) {
    			throw new ComponentNotReadyException("Unable to read metadata definition from CloverData file", e);
    		}
        	
        	if (metadata != null) { // CLO-5416: can also be used to read metadata from Clover debug file
        		// CLO-4591:
        		DataRecordMetadata nonAutofilledFieldsMetadata = MetadataUtils.getNonAutofilledFieldsMetadata(metadata);
        		if (!nonAutofilledFieldsMetadata.equals(version.metadata, false)) {
        			logger.error("Data structure of input file is not compatible with used metadata. File data structure: " + version.metadata.toStringDataTypes());
        			throw new ComponentNotReadyException("Data structure of input file is not compatible with used metadata. More details available in log.");
        		}
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
		final int size;
		try {
			size=ByteBufferUtils.decodeLength(input);
			if (size<0) return null; //end of file reached
		
			recordBuffer.clear();

			recordBuffer.limit(recordBuffer.position() + size);
			if (input.read(recordBuffer)==-1){
				throw new JetelException("Insufficient data in datastream.");
			}
			
			recordBuffer.flip();
		} catch(IOException ex){
			throw new JetelException(ex);
		}
		
		record.deserializeUnitary(recordBuffer, serializer);
		
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
		if (!version.raw) return -1;
		final int size;
		try {
			size=ByteBufferUtils.decodeLength(input);
			if (size<0) return 0; //end of file reached
		
			targetBuffer.clear();

			// CLO-2657:
			//in case current transformation is jobflow, tokenId must be added to targetBuffer
			//since tokenId is not part of clover data file
			if (isJobflow) {
				Token.serializeTokenId(-1, targetBuffer);
			}

			targetBuffer.limit(targetBuffer.position() + size);
			if (input.read(targetBuffer)==-1){
				throw new JetelException("Insufficient data in datastream.");
			}
			
			targetBuffer.flip();
		} catch(IOException ex){
			throw new JetelException(ex);
		}
		
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
	public boolean isDirectReadingSupported() {
		return true;
	}


	@Override
	public DataSourceType getPreferredDataSourceType() {
		return DataSourceType.STREAM;
	}
	
	
	
}
