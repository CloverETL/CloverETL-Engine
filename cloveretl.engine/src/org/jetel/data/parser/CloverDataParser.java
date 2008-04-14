
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

package org.jetel.data.parser;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.ByteBufferUtils;

/**
 * Class for reading data saved in Clover internal format
 * It is predicted that zip file (with name dataFile.zip) has following structure:
 * DATA/dataFile
 * INDEX/dataFile.idx
 * If data are not in zip file, indexes (if needed) have to be in the same location
 * 
 * @author avackova <agata.vackova@javlinconsulting.cz> ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 13, 2006
 *
 */
public class CloverDataParser implements Parser {
	
	private DataRecordMetadata metadata;
	private InputStream in;
	private ReadableByteChannel recordFile;
	private ByteBuffer recordBuffer;
	private long index = 0;//index for reading index
	private long idx = 0;//index for reading record
	private int recordSize;
	private String indexFileURL;
	private int compressedData = -1;
	private String inData;
	
	private final static int LONG_SIZE_BYTES = 8;
    private final static int LEN_SIZE_SPECIFIER = 4;

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getNext()
	 */
	public DataRecord getNext() throws JetelException {
		DataRecord record = new DataRecord(metadata);
		record.init();
		return getNext(record);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#skip(int)
	 */
	public int skip(int nRec) throws JetelException {
		index = LONG_SIZE_BYTES * nRec;
		return nRec;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#init(org.jetel.metadata.DataRecordMetadata)
	 */
	public void init(DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		this.metadata = _metadata;
        recordBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	public void setReleaseDataSource(boolean releaseInputSource)  {
	}

    /* (non-Javadoc)
     * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
     * 
     * parameter: data fiele name or {data file name, index file name}
     */
    public void setDataSource(Object in) throws ComponentNotReadyException {
        if (in instanceof String[]) {
        	inData = ((String[])in)[0];
        	indexFileURL = ((String[])in)[1];
        }else{
        	inData = (String)in;
        	indexFileURL = null;
        }
    	String fileName = new File(inData).getName();
        if ((inData).endsWith(".zip")) {
            fileName = fileName.substring(0,fileName.lastIndexOf('.')); 
        }
        //set input stream
        try {
            switch (compressedData) {
            case 1:
                this.in = new ZipInputStream(new FileInputStream(inData));
                break;
            case 0:
                this.in = new FileInputStream(inData);
                if (inData.endsWith(".zip")) {
                	fileName+=".zip";
                }
                break;
            default:
                if ((inData).endsWith(".zip")){
                    this.in = new ZipInputStream(new FileInputStream(inData));
                    compressedData = 1;
                }else{
                    this.in = new FileInputStream(inData);
                    compressedData = 0;
                }
                break;
            }
            if (this.in instanceof ZipInputStream){
                ZipEntry entry;
                //find entry DATA/fileName
                while((entry = ((ZipInputStream)this.in).getNextEntry()) != null) {
                    if(entry.getName().equals(
                    		CloverDataFormatter.DATA_DIRECTORY + fileName)) {
                        break;
                    }
                }
            }
            recordFile = Channels.newChannel(this.in);
            if (index > 0) {//reading not all records --> find index in record file
            	setStartIndex(fileName);
            }
            //skip idx bytes from record file
            int i=0;
            do {
                ByteBufferUtils.reload(recordBuffer,recordFile);
                recordBuffer.flip();
                i++;
            }while (i*Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE < idx);
            recordBuffer.position((int)idx%Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
        } catch (IOException ex) {
            throw new ComponentNotReadyException(ex);
        }
    }

    private void setStartIndex(String fileName) throws IOException, ComponentNotReadyException{
        DataInputStream indexFile;
        if (this.in instanceof ZipInputStream){//read index from archive
            ZipInputStream tmpIn = new ZipInputStream(new FileInputStream(inData));
            indexFile = new DataInputStream(tmpIn);
            ZipEntry entry;
            //find entry INDEX/fileName.idx
            while((entry = tmpIn.getNextEntry()) != null) {
                if(entry.getName().equals(
                        CloverDataFormatter.INDEX_DIRECTORY + fileName + 
                        CloverDataFormatter.INDEX_EXTENSION)) {
                    indexFile.skip(index);
                    try {
						idx = indexFile.readLong();//read index for reading records
					} catch (EOFException e) {
			            tmpIn.close();
			            indexFile.close();
						throw new ComponentNotReadyException("Start record is greater than last record!!!");
					}
                    break;
                }
            }
            tmpIn.close();
            indexFile.close();
        }else{//read index from binary file
            if (indexFileURL == null){
                File dir = new File(inData).getParentFile();
                indexFile = new DataInputStream(new FileInputStream(
                		(dir != null ? dir.getAbsolutePath() + CloverDataFormatter.FILE_SEPARATOR : "") 
                        	+ fileName + CloverDataFormatter.INDEX_EXTENSION));
            }else{
                indexFile = new DataInputStream(new FileInputStream(indexFileURL));
            }
            indexFile.skip(index);
            idx = indexFile.readLong();
            indexFile.close();
        }
    }
    
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#close()
	 */
	public void close() {
		if (recordFile != null) {
			try {
				recordFile.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getNext(org.jetel.data.DataRecord)
	 */
	public DataRecord getNext(DataRecord record)throws JetelException{
		//refill buffer if we are on the end of buffer
		if (recordBuffer.remaining() < LEN_SIZE_SPECIFIER) {
			try {
				ByteBufferUtils.reload(recordBuffer,recordFile);
				recordBuffer.flip();
			} catch (IOException e) {
				throw new JetelException(e.getLocalizedMessage());
			}
		}
		if (recordBuffer.remaining() < LEN_SIZE_SPECIFIER){
			return null;
		}
		recordSize = recordBuffer.getInt();
		//refill buffer if we are on the end of buffer
		if (recordBuffer.remaining() < recordSize ){
			try{
				ByteBufferUtils.reload(recordBuffer,recordFile);
				recordBuffer.flip();
			}catch(IOException ex){
				throw new JetelException(ex.getLocalizedMessage());
			}
		}
		if (recordBuffer.remaining() < recordSize) {
			throw new RuntimeException("The size of data buffer is only " + recordBuffer.limit() + 
					", but record size is " + recordSize + ". Set appropriate parameter in defautProperties file.");
		}
		record.deserialize(recordBuffer);
		return record;
	}
 
	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setExceptionHandler(org.jetel.exception.IParserExceptionHandler)
	 */
	public void setExceptionHandler(IParserExceptionHandler handler) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getExceptionHandler()
	 */
	public IParserExceptionHandler getExceptionHandler() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getPolicyType()
	 */
	public PolicyType getPolicyType() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setCompressedData(int compressedData) {
		this.compressedData = compressedData;
	}

	private void resetInternal() throws IOException, ComponentNotReadyException{
		if (recordFile.isOpen()) {
			recordFile.close();
		}
		recordBuffer.clear();
    	String fileName = new File(inData).getName();
        if (this.in instanceof ZipInputStream){
            this.in = new ZipInputStream(new FileInputStream(inData));
            if (inData.endsWith(".zip")) {
            	fileName = fileName.substring(0, fileName.length() - ".zip".length());
            }
            ZipEntry entry;
            //find entry DATA/fileName
            while((entry = ((ZipInputStream)this.in).getNextEntry()) != null) {
                if(entry.getName().equals(
                		CloverDataFormatter.DATA_DIRECTORY + fileName)) {
                    break;
                }
            }
        }else{
        	this.in = new FileInputStream(inData);
        }
        recordFile = Channels.newChannel(this.in);
        if (index > 0) {//reading not all records --> find index in record file
        	setStartIndex(fileName);
        }
        //skip idx bytes from record file
        int i=0;
        do {
            ByteBufferUtils.reload(recordBuffer,recordFile);
            recordBuffer.flip();
            i++;
        }while (i*Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE < idx);
        recordBuffer.position((int)idx%Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
	}

	public void reset() {
		try{
			resetInternal();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Object getPosition() {
		// TODO Auto-generated method stub
		return null;
	}

	public void movePosition(Object position) {
		// TODO Auto-generated method stub
		
	}
}
