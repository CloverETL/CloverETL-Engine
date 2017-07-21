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
package org.jetel.data.formatter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.jetel.data.CloverDataRecordSerializer;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordSerializer;
import org.jetel.data.Defaults;
import org.jetel.data.parser.CloverDataParser;
import org.jetel.data.parser.CloverDataParser.FileConfig;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.util.JetelVersion;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.primitive.BitArray;
import org.jetel.util.stream.CloverDataStream;

/**
 * Class for saving data in Clover internal format
 * Data are saved to zip file with structure:
 * DATA/fileName
 * INDEX/fileName.idx
 * METADATA/fileName.fmt
 * or to binary files
 * 
 * 
 * @author avackova <agata.vackova@javlinconsulting.cz> ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 12, 2006
 *
 */
public class CloverDataFormatter extends AbstractFormatter {
	

	/**
	 * This long value is used as a header for internal clover binary data sources/targets.
	 * CloverDataReader and CloverDataWriter components are dedicated to work with this data format.
	 * Each clover data source (since 2.9 version) starts with this value and follows
	 * with compatibility value @see CLOVER_DATA_COMPATIBILITY_HASH, one byte for major version number,
	 * one byte for minor version number, one byte for revision number, and other four bytes
	 * for various options.
	 * NOTE: cannot be changed from defaultProperties file
	 */
	public final static long CLOVER_DATA_HEADER = 7198760165196065077L;

	/**
	 * This long value is used for decision about inter-version compatibility
	 * of clover binary format. Need to be changed whenever clover engine changed way how to
	 * data records are serialized.
	 * NOTE: cannot be changed from defaultProperties file
	 */
	public final static long CLOVER_DATA_COMPATIBILITY_HASH_2_9 = 620003156160528134L;
	public final static long CLOVER_DATA_COMPATIBILITY_HASH_3_5 = 7252194213196531926L;
	public final static long CLOVER_DATA_COMPATIBILITY_HASH_4_0 = 3154188142006786177L;

	public final static DataFormatVersion CURRENT_FORMAT_VERSION = DataFormatVersion.VERSION_40;
	
	public final static ByteOrder BUFFER_BYTE_ORDER = ByteOrder.BIG_ENDIAN;
	public final static DataIndexTableSize DEFAULT_BLOCK_INDEX_SIZE = DataIndexTableSize.S128;
	public final static DataCompressAlgorithm DEFAULT_COMPRESSION_ALGORITHM = DataCompressAlgorithm.LZ4;
	
	private final static short LEN_SIZE_SPECIFIER = 4;
	private final static int LONG = 8;
	private final static int BYTE = 1;
	
	/**
	 * Various Options on/off as bits in header (masks)
	 */
	public static final int HEADER_OPTIONS_ARRAY_SIZE = 8;
	public static final int HEADER_OPTIONS_ARRAY_SIZE_3_5 = 4;
	public static final long OPTION_MASK_USE_FIELDS_NULL_INDICATORS = 0b01; //bit 1
	public static final long OPTION_MASK_COMPRESSED_DATA = 0b1110;  // bits 2..4 one of compressions 000 -> none 001 -> LZ4 010..111 -> reserved
	public static final long OPTION_MASK_STORE_INDEX_DATA = 0b1110000;  // bits 5..7 size of index (entries) stored 00 - none 001 -> 64 010 ->128 011->256 
	public static final long OPTION_MASK_RAW_DATA = 0b10000000;  // bit 8: raw data (1 -> raw data, 0 -> custom serialization) 
	
	public final static int CLOVER_DATA_HEADER_LENGTH= 
			LONG +  // data header
			LONG +  // compatibility hash
			BYTE +  // major ver
			BYTE +  // minor ver
			BYTE;  // revision ver
	
	public final static int CLOVER_DATA_HEADER_OPTIONS_OFFSET= CLOVER_DATA_HEADER_LENGTH -1;
	
	public final static char FILE_SEPARATOR = '/';
	public final static String DATA_DIRECTORY = "DATA" + FILE_SEPARATOR;
	public final static String INDEX_DIRECTORY = "INDEX" + FILE_SEPARATOR;
	public final static String METADATA_DIRECTORY = "METADATA" + FILE_SEPARATOR;
	public final static String INDEX_EXTENSION = ".idx";
	public final static String METADATA_EXTENSION = ".fmt";
	public final static String TMP_EXTENSION = ".tmp";

	private WritableByteChannel writer;
	private CloverDataStream.Output output;
	private OutputStream out;//FileOutputStream or ZipOutputStream
	private CloverBuffer buffer;
	private boolean saveIndex;
	private String fileURL;
	private String fileName;
	private boolean isOpen = false;
	private DataCompressAlgorithm compress;
	private boolean raw = true;
	private URL projectURL;
	private DataRecordMetadata metadata;	
	
	private DataRecordSerializer serializer;


	
	/**
	 * Constructor
	 * 
	 * @param fileName name of archive or name of binary file with records
	 * @param saveIndex whether to save indexes of records or not 
	 */
	public CloverDataFormatter(String fileName,boolean saveIndex) {
		this.fileURL = fileName;
		this.saveIndex = saveIndex;
		this.compress= DataCompressAlgorithm.NONE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#init(org.jetel.metadata.DataRecordMetadata)
	 */
	@Override
	public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
		this.metadata = metadata;
        buffer = CloverBuffer.allocateDirect(Defaults.Record.RECORDS_BUFFER_SIZE+LEN_SIZE_SPECIFIER);
        buffer.order(BUFFER_BYTE_ORDER);
        serializer = new CloverDataRecordSerializer();
 	}

    /* (non-Javadoc)
     * @see org.jetel.data.formatter.Formatter#setDataTarget(java.lang.Object)
     */
    @Override
	public void setDataTarget(Object outputDataTarget) throws IOException {
		buffer.clear();
		// create output stream
		if (outputDataTarget instanceof OutputStream){
			this.out = (OutputStream) outputDataTarget;
			writer = Channels.newChannel(this.out);
		}else if (outputDataTarget instanceof File){
			this.fileName=((File)outputDataTarget).getAbsolutePath();
			this.writer = new RandomAccessFile((File)outputDataTarget,"rw").getChannel();
			this.out = Channels.newOutputStream(this.writer);
		}else{
        	throw new IOException("Unsupported  Data Target type: "+outputDataTarget.getClass().getName());
        }
		isOpen = true;
		boolean writeHeader = true;
		if (append) {
			if (!(writer instanceof SeekableByteChannel)) {
				throw new RuntimeException("Seekable stream is required for appending. Got "+writer.getClass().getName());
			}
			try {
				if (((SeekableByteChannel) writer).size() > 0) {
					FileConfig version = CloverDataParser.checkCompatibilityHeader((SeekableByteChannel)writer, metadata);
					//check that we have compatible format version
					if (version.formatVersion!=CURRENT_FORMAT_VERSION){
						throw new IOException("Can not append. Target file is of incompatible version - "+version.formatVersion);
					}
					this.compress = DataCompressAlgorithm.getAlgorithm(version.compressionAlgorithm);
					this.raw = version.raw;
					writeHeader = false;
				} else {
					// write header information for compatibility testing while later reading
					writeHeader = true;
					append=false; //zero length, thus no appending actually
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (ComponentNotReadyException e) {
				throw new RuntimeException(e);
			}
		}
		try {
			int size = 0;
			if (writeHeader) {
				// write header information for compatibility testing while later reading
				writeCompatibilityHeader();
				buffer.flip();
				size = buffer.remaining();
				writer.write(buffer.buf());
			}

			switch (compress) {
			case NONE:
				this.output = new CloverDataStream.Output(out);
				this.output.setCompress(false);
				break;
			case LZ4:
				this.output = new CloverDataStream.Output(out, CloverDataStream.Output.DEFAULT_BLOCK_SIZE, new CloverDataStream.CompressorLZ4());
				this.output.setCompress(true);
				break;
			case GZIP:
				this.output = new CloverDataStream.Output(out, CloverDataStream.Output.DEFAULT_BLOCK_SIZE, new CloverDataStream.CompressorGZIP());
				this.output.setCompress(true);
				break;
			default:
				throw new RuntimeException("Unsupported compression algorithm: " + compress);
			}
			if (append) {
				this.output.seekToAppend((SeekableByteChannel) writer);
			} else {
				this.output.setPosition(size); // need to tell CloverDataStream what is current position of the wrapped
												// stream
			}
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}
    
    @Override
	public DataTargetType getPreferredDataTargetType() {
		return DataTargetType.FILE;
	}

    private void writeCompatibilityHeader() {
        //write a clover data binary header @see Defaults.Component.CLOVER_DATA_HEADER
        //HEADER & COMPATIBILITY_HASH & MAJOR_VERSION & MINOR_VERSION & REVISION_VERSION & 8_EXTRA_BYTES
        buffer.putLong(CLOVER_DATA_HEADER);
        buffer.putLong(CLOVER_DATA_COMPATIBILITY_HASH_4_0);
        buffer.put((byte) JetelVersion.getMajorVersion());
        buffer.put((byte) JetelVersion.getMinorVersion());
        buffer.put((byte) JetelVersion.getRevisionVersion());
        //extra bytes now used only first bit to distinquish whether null fields are serialized as a bit array
        //@see Defaults.Record.USE_FIELDS_NULL_INDICATORS
    	byte[] extraBytes = new byte[HEADER_OPTIONS_ARRAY_SIZE];
        if (Defaults.Record.USE_FIELDS_NULL_INDICATORS) {
        	BitArray.encodeNumber(extraBytes, OPTION_MASK_USE_FIELDS_NULL_INDICATORS,1);
        }
        //encode compression
        BitArray.encodeNumber(extraBytes, OPTION_MASK_COMPRESSED_DATA, compress.getId());
        if (isSaveIndex()){
        	BitArray.encodeNumber(extraBytes, OPTION_MASK_STORE_INDEX_DATA, DEFAULT_BLOCK_INDEX_SIZE.getId());
        }
        BitArray.encodeNumber(extraBytes, OPTION_MASK_RAW_DATA, raw ? 1 : 0);
        
        buffer.put(extraBytes);
        //serialize used metadata into data file - will be used to validate input file by CloverDataParser 
        byte[] metaser = metadataSerialize(metadata);
        //encode serialized metadata length
        ByteBufferUtils.encodeLength(buffer,metaser.length);
        buffer.put(metaser);
    }
    
    @Override
	public void reset() {
		if (isOpen) {
			try {
				close();
			} catch (IOException e) {
				throw new JetelRuntimeException(e);
			}
		}
	}
	
	@Override
	public void finish() throws IOException{
    	if (!isOpen) return;
    	flush();
    }
    
	/* (non-Javadoc)//			writer.close();

	 * @see org.jetel.data.formatter.Formatter#close()
	 */
	@Override
	public void close() throws IOException {
		if (!isOpen) return;
		output.close();
		if (writer.isOpen()) {
			writer.close();
		}
		isOpen = false;
	}
	
	
	/**
	 * If enabled, saves the record size to the index file.
	 * 
	 * The functionality is deprecated.
	 * 
	 * @param recordSize
	 * @throws IOException
	 */
	@Override
	public int write(DataRecord record) throws IOException {
		buffer.clear();
		record.serialize(buffer, serializer);
		buffer.flip();
		return writeDirect(buffer);
	}

	/**
	 * Copies the data from the <code>recordBuffer</code>
	 * to the output.
	 * 
	 * It is assumed that the <code>recordBuffer</code>
	 * is prepared for reading and has a limit set correctly.
	 * 
	 * @param recordBuffer
	 * @return
	 * @throws IOException
	 */
	public int writeDirect(CloverBuffer recordBuffer) throws IOException {
		int recordSize = recordBuffer.remaining();
		output.markRecordStart();
		final int lenbytes=ByteBufferUtils.encodeLength(output, recordSize);
		output.write(recordBuffer);
        return recordSize + lenbytes;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#flush()
	 */
	@Override
	public void flush() throws IOException {
		//output.flush();
		//out.flush();
	}
	
	public boolean isSaveIndex() {
		return saveIndex;
	}

	@Override
	public int writeFooter() throws IOException {
		return 0;
	}

	@Override
	public int writeHeader() throws IOException {
		return 0;
	}

	public void setProjectURL(URL projectURL) {
		this.projectURL = projectURL;
	}

	
	
	protected byte[] metadataSerialize(DataRecordMetadata metadata) {
		ByteArrayOutputStream  outStream = new ByteArrayOutputStream();
		DataRecordMetadataXMLReaderWriter.write(metadata, outStream);
		return outStream.toByteArray();
	}

	/**
	 * @return the compress
	 */
	public boolean isCompressData() {
		return this.compress!=DataCompressAlgorithm.NONE;
	}
	
	public boolean isRawData() {
		return this.raw;
	}

	/**
	 * @param compress the compress to set
	 */
	public void setCompressLevel(int compressLevel) {
		if (compressLevel == 0) {
			this.compress = DataCompressAlgorithm.NONE;
		} else if (compressLevel < 7) {
			this.compress = DataCompressAlgorithm.LZ4;
		} else {
			this.compress = DataCompressAlgorithm.GZIP;
		}
		if (compressLevel > 1) {
			raw = false;
		}
	}
	
public enum DataCompressAlgorithm {
		
		NONE(0),
		LZ4(1),
		GZIP(2);
		
		private int id;
		
		DataCompressAlgorithm(int algorithm){
			this.id=algorithm;
		}
		
		public int getId(){
			return id;
		}
		
		public static DataCompressAlgorithm getAlgorithm(int id){
			switch(id){
			case 0: return NONE;
			case 1: return LZ4;
			case 2: return GZIP;
			default:
				return NONE;
			}
		}
	}	
		
	public enum DataIndexTableSize {
		
		NONE(0,0),
		S64(64,1),
		S128(128,2),
		S256(256,3),
		S512(512,4);
		
		private int size;
		private int id;
		
		DataIndexTableSize(int size,int id){
			this.id=id;
			this.size=size;
		}
		
		public int getId(){
			return id;
		}
		
		public int getSize(){
			return size;
		}
	}	
	
	public enum DataFormatVersion{
		VERSION_29, VERSION_35, VERSION_40;
	}
	
}
