
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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
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
	private InputStream in;
	private ReadableByteChannel recordFile;
	private ByteBuffer recordBuffer;
	private long index = 0;
	private long idx = 0;
	private int recordSize;
	
	private final static int LONG_SIZE_BYTES = 8;
    private final static int LEN_SIZE_SPECIFIER = 4;

	/**
	 * @param fileurl
	 */
	public CloverDataParser() {
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
		index = LONG_SIZE_BYTES * nRec;
		return nRec;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object in, DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		this.metadata = _metadata;
		String fileName;
		try {
			if (((String)in).endsWith(".zip")){
				this.in = new ZipInputStream(new FileInputStream((String)in));
				fileName = ((String)in).substring(((String)in).lastIndexOf(File.separator)+1,((String)in).lastIndexOf('.'));
			}else{
				this.in = new FileInputStream((String)in);
				fileName  = ((String)in).substring(((String)in).lastIndexOf(File.separator)+1);
			}
			recordFile = Channels.newChannel(this.in);
			recordBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
			if (index > 0) {
				if (((String)in).endsWith(".zip")){
					ZipInputStream tmpIn = new ZipInputStream(new FileInputStream((String)in));
		            ZipEntry entry;
		            byte[] readBuffer = new byte[LONG_SIZE_BYTES];
		            while((entry = tmpIn.getNextEntry()) != null) {
		                if(entry.getName().equals("INDEX/"+fileName+".idx")) {
		                	tmpIn.skip(index);
		                	tmpIn.read(readBuffer);
		                	break;
		                }
		            }
		            idx = (((long)readBuffer[0] << 56) +
		                    ((long)(readBuffer[1] & 255) << 48) +
		                    ((long)(readBuffer[2] & 255) << 40) +
		                    ((long)(readBuffer[3] & 255) << 32) +
		                    ((long)(readBuffer[4] & 255) << 24) +
		                    ((readBuffer[5] & 255) << 16) +
		                    ((readBuffer[6] & 255) <<  8) +
		                    ((readBuffer[7] & 255) <<  0));
		            tmpIn.close();
				}else{
					File root = new File(((String)in)).getParentFile().getParentFile();
					String filePath = root == null ? "" : root.getPath() + File.separator;
					DataInputStream indexFile = new DataInputStream(new FileInputStream(
							filePath + "INDEX" + File.separator + fileName + ".idx"));
					indexFile.skip(index);
					idx = indexFile.readLong();
					indexFile.close();
				}
			}
			if (((String)in).endsWith(".zip")) {
	            ZipEntry entry;
	            while((entry = ((ZipInputStream)this.in).getNextEntry()) != null) {
	                if(entry.getName().equals("DATA/"+fileName)) {
	                	break;
	                }
	            }
			}
			int i=0;
			do {
				recordBuffer.rewind();
				recordFile.read(recordBuffer);
				i++;
			}while (i*Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE < idx);
			recordBuffer.position((int)idx%Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		} catch (IOException ex) {
			throw new ComponentNotReadyException(ex);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#close()
	 */
	public void close() {
		try {
			recordFile.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#getNext(org.jetel.data.DataRecord)
	 */
	public DataRecord getNext(DataRecord record)throws JetelException{
		if (recordBuffer.remaining() < LEN_SIZE_SPECIFIER) {
			recordBuffer.compact();
			try{
				recordFile.read(recordBuffer);
			}catch(IOException ex){
				throw new JetelException(ex.getLocalizedMessage());
			}
			recordBuffer.flip();
		}
		if (recordBuffer.remaining() < LEN_SIZE_SPECIFIER){
			return null;
		}
		recordSize = recordBuffer.getInt();
		if (recordSize > recordBuffer.remaining()){
			recordBuffer.compact();
			try{
				recordFile.read(recordBuffer);
			}catch(IOException ex){
				throw new JetelException(ex.getLocalizedMessage());
			}
			recordBuffer.flip();
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

}
