
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import org.jetel.data.DataRecord;
import org.jetel.data.tape.DataRecordTape;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author avackova <agata.vackova@javlinconsulting.cz> ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 13, 2006
 *
 */
public class CloverDataParser implements Parser {
	
	private DataRecordMetadata metadata;
	private DataRecordTape tape;
	private FileChannel recordFile;
	private ByteBuffer recordBuffer;
	private RandomAccessFile indexFile;
	private long index;
	private long idx;
	private int startRecord = 0;
	private int counter;
	
	private final static int LONG_SIZE_BYTES = 8;
	private final static short BUFFER_CAPACITY = 100;
    private final static int LEN_SIZE_SPECIFIER = 4;

	public int getStartRecord() {
		return startRecord;
	}

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
		this.startRecord = nRec;
		index = LONG_SIZE_BYTES * nRec;
		return nRec;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object in, DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		this.metadata = _metadata;
		if (startRecord == 0) {
			tape = new DataRecordTape((String)in,false,false);
			try{
				tape.open();
			}catch(IOException ex){
				throw new ComponentNotReadyException(ex);
			}
			tape.addDataChunk();
			tape.rewind();
		}else{
			try {
				DataRecord dr = new DataRecord(metadata);
				dr.init();
				recordFile = new RandomAccessFile((String)in,"r").getChannel();
				recordBuffer = ByteBuffer.allocateDirect((dr.getSizeSerialized()+LEN_SIZE_SPECIFIER)*BUFFER_CAPACITY);
				indexFile = new RandomAccessFile((String)in + ".idx","r");
				indexFile.seek(index);
				idx = indexFile.readLong();
				recordFile.position(idx);
				counter = 0;
			}catch(IOException ex){
				throw new ComponentNotReadyException(ex);
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#close()
	 */
	public void close() {
		try{
			if (startRecord == 0) {
				tape.close();
			}else{
				recordFile.close();
				indexFile.close();
			}
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getNext(org.jetel.data.DataRecord)
	 */
	public DataRecord getNext(DataRecord record) throws JetelException {
		try {
			if (startRecord == 0) {
				return getNextFromTape(record);
			}else{
				return getNextFromFile(record);
			}
		}catch(IOException ex){
			throw new JetelException(ex.getLocalizedMessage());
		}
	}
	
	private DataRecord getNextFromTape(DataRecord record) throws IOException{
        if (tape.get(record)){
            return record;
        }else{
            return null;
        }
	}
	
	private DataRecord getNextFromFile(DataRecord record)throws IOException{
		if (counter % BUFFER_CAPACITY == 0){
			recordFile.read(recordBuffer);
			recordBuffer.rewind();
		}
		recordBuffer.position(recordBuffer.position()+LONG_SIZE_BYTES);
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

}
