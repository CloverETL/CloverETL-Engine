
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

package org.jetel.data.formatter;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;


/**
 * @author avackova <agata.vackova@javlinconsulting.cz> ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 12, 2006
 *
 */
public class CloverDataFormatter implements Formatter {
	
	private WritableByteChannel writer;
	private OutputStream out;
	private ByteBuffer buffer;
	private DataRecordMetadata metadata;
	private WritableByteChannel idxWriter;
	private ByteBuffer idxBuffer;
	private boolean saveIndex;
	private long index = 0;
	private int recordSize;
	private boolean opened = false;
	private String fileURL;
	private String fileName;
	private File idxTmpFile;

	private final static int LEN_SIZE_SPECIFIER = 4;
	private final static int LONG_SIZE_BYTES = 8;

	
	public CloverDataFormatter(String fileName,boolean saveIndex) {
		this.fileURL = fileName;
		this.saveIndex = saveIndex;
		this.fileName = fileURL.substring(fileURL.lastIndexOf(File.separatorChar)+1);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object out, DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		this.metadata = _metadata;
		if (out instanceof ZipOutputStream) {
			this.out = (ZipOutputStream)out;
		}else{
			this.out = (FileOutputStream)out;
		}
		writer = Channels.newChannel(this.out);
		buffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		if (saveIndex) {
			File dataDir = new File(fileURL.substring(0,fileURL.lastIndexOf(File.separatorChar)+1) + "INDEX");
			dataDir.mkdir();
			idxTmpFile = new File(dataDir.getPath() + File.separator + fileName  + ".idx");
			try{
				idxWriter = Channels.newChannel(new DataOutputStream(
						new FileOutputStream(idxTmpFile)));
			}catch(IOException ex){
				throw new ComponentNotReadyException(ex);
			}
			idxBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		}
	}


	
	/* (non-Javadoc)//			writer.close();

	 * @see org.jetel.data.formatter.Formatter#close()
	 */
	public void close() {
		try{
			flush();
			if (out instanceof ZipOutputStream) {
				((ZipOutputStream)out).closeEntry();
				((ZipOutputStream)out).putNextEntry(new ZipEntry("INDEX/" + fileName + ".idx"));
				if (idxTmpFile.length() == 0) {
					writer.write(idxBuffer);
				}else{
					idxWriter.close();
					ReadableByteChannel idxReader = Channels.newChannel(
							new FileInputStream(idxTmpFile));
					while (idxReader.read(buffer) > 0){
						flush();
					}
					idxBuffer.flip();
					writer.write(idxBuffer);
				}
				writer.close();
				idxTmpFile.delete();
				idxTmpFile.getParentFile().delete();
			}else{
				flushIdxBuffer();
			}
			out.close();
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#write(org.jetel.data.DataRecord)
	 */
	public void write(DataRecord record) throws IOException {
		if (out instanceof ZipOutputStream && !opened) {
			((ZipOutputStream)out).putNextEntry(new ZipEntry("DATA/" + fileName));
			opened = true;
		}
		if (idxBuffer.remaining() < LONG_SIZE_BYTES){
			flushIdxBuffer();
		}
		idxBuffer.putLong(index);
		recordSize = record.getSizeSerialized();
		if (buffer.remaining() < recordSize + LEN_SIZE_SPECIFIER){
			flush();
		}
		buffer.putInt(recordSize);
		record.serialize(buffer);
		index+= recordSize + LEN_SIZE_SPECIFIER;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#flush()
	 */
	public void flush() throws IOException {
		buffer.flip();
		writer.write(buffer);
		buffer.clear();
	}
	
	private void flushIdxBuffer()throws IOException{
		idxBuffer.flip();
		idxWriter.write(idxBuffer);
		idxBuffer.clear();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#setOneRecordPerLinePolicy(boolean)
	 */
	public void setOneRecordPerLinePolicy(boolean b) {
		// TODO Auto-generated method stub

	}

	/**
	 * @param index
	 */

}
