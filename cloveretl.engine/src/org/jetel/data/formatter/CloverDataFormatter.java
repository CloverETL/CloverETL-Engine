
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

import java.io.DataInputStream;
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
import org.jetel.util.ByteBufferUtils;


/**
 * Class for saving data in Clover internal format
 * Data are saved to zip file with structure:
 * DATA/fileName
 * INDEX/fileName.idx
 * METADATA/fileName.fmt
 * 
 * 
 * 
 * @author avackova <agata.vackova@javlinconsulting.cz> ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 12, 2006
 *
 */
//TODO change inplementation when out is ZipOutputStream and append=true
public class CloverDataFormatter implements Formatter {
	
	private WritableByteChannel writer;
	private OutputStream out;
	private ByteBuffer buffer;
	private DataRecordMetadata metadata;
	private WritableByteChannel idxWriter;
	private ByteBuffer idxBuffer;
	private boolean saveIndex;
	private short index = 0;
	private int recordSize;
	private String fileURL;
	private String fileName;
	private File idxTmpFile;
	private ReadableByteChannel idxReader;
	private boolean append;
	
	private final static short LEN_SIZE_SPECIFIER = 4;
	private final static int SHORT_SIZE_BYTES = 2;
	private final static int LONG_SIZE_BYTES = 8;

	
	public CloverDataFormatter(String fileName,boolean saveIndex) {
		this.fileURL = fileName;
		this.saveIndex = saveIndex;
		if (fileURL.toLowerCase().endsWith(".zip")){
			this.fileName = fileURL.substring(fileURL.lastIndexOf(File.separatorChar)+1,fileURL.lastIndexOf('.'));
		}else{
			this.fileName = fileURL.substring(fileURL.lastIndexOf(File.separatorChar)+1);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#open(java.lang.Object, org.jetel.metadata.DataRecordMetadata)
	 */
	public void open(Object out, DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		this.metadata = _metadata;
		if (out instanceof ZipOutputStream) {
			this.out = (ZipOutputStream)out;
			try {
				((ZipOutputStream)out).putNextEntry(new ZipEntry("DATA/" + fileName));
			}catch(IOException ex){
				throw new ComponentNotReadyException(ex);
			}
		}else{
			this.out = (FileOutputStream)out;
		}
		writer = Channels.newChannel(this.out);
		buffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		if (saveIndex) {
			File dataDir = new File(fileURL.substring(0,fileURL.lastIndexOf(File.separatorChar)+1) + "INDEX");
			dataDir.mkdir();
			idxTmpFile = new File(dataDir.getPath() + File.separator + fileName  + ".idx.tmp");
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
			if (saveIndex) {
				idxReader = new FileInputStream(idxTmpFile).getChannel();
				if (idxTmpFile.length() > 0){
					ByteBufferUtils.flush(idxBuffer,idxWriter);
					idxWriter.close();
				}else{
					idxBuffer.flip();
				}
				long startValue = 0;
				int position;
				if (out instanceof ZipOutputStream) {
					((ZipOutputStream)out).closeEntry();
					((ZipOutputStream)out).putNextEntry(new ZipEntry("INDEX/" + fileName + ".idx"));
					do {
						startValue = changSizeToIndex(startValue);
						position = buffer.position();
						flush();
					}while (position == buffer.limit());
					((ZipOutputStream)out).closeEntry();
					idxTmpFile.delete();
					idxTmpFile.getParentFile().delete();
				}else{
					out.close();
					File idxInFile = new File(idxTmpFile.getCanonicalPath().substring(0,idxTmpFile.getCanonicalPath().lastIndexOf('.')));
					if (append && idxInFile.length() > 0) {
						DataInputStream idxIn = new DataInputStream(new FileInputStream(idxInFile));
						idxIn.skip(idxInFile.length() - LONG_SIZE_BYTES);
						startValue = idxIn.readLong();
						idxIn.close();
					}
					idxWriter = new FileOutputStream(idxInFile,append).getChannel();
					do {
						startValue = changSizeToIndex(startValue);
						position = buffer.position();
						ByteBufferUtils.flush(buffer,idxWriter);
					}while (position == buffer.limit());
					idxTmpFile.delete();
					idxWriter.close();
				}
			}else{
				if (out instanceof ZipOutputStream) {
					((ZipOutputStream)out).closeEntry();
				}else{
					out.close();
				}
			}
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}
	
	private long changSizeToIndex(long lastValue) throws IOException{
		while (buffer.remaining() >= LONG_SIZE_BYTES){
			if (idxBuffer.remaining() < SHORT_SIZE_BYTES && idxBuffer.limit() == idxBuffer.capacity()){
				idxBuffer.limit(ByteBufferUtils.reload(idxBuffer,idxReader));
			}
			if (idxBuffer.remaining() < SHORT_SIZE_BYTES ){
				break;
			}
			buffer.putLong(lastValue);
			lastValue += idxBuffer.getShort();
		}
		return lastValue;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#write(org.jetel.data.DataRecord)
	 */
	public void write(DataRecord record) throws IOException {
		recordSize = record.getSizeSerialized();
		if (saveIndex) {
			index = recordSize + LEN_SIZE_SPECIFIER <= Short.MAX_VALUE ? 
					(short)(recordSize + LEN_SIZE_SPECIFIER) : 
					(short)(Short.MAX_VALUE - (recordSize + LEN_SIZE_SPECIFIER));
			if (idxBuffer.remaining() < SHORT_SIZE_BYTES){
				ByteBufferUtils.flush(idxBuffer,idxWriter);
			}
			idxBuffer.putShort(index);
		}
		if (buffer.remaining() < recordSize + LEN_SIZE_SPECIFIER){
			flush();
		}
		buffer.putInt(recordSize);
		record.serialize(buffer);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#flush()
	 */
	public void flush() throws IOException {
		ByteBufferUtils.flush(buffer,writer);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#setOneRecordPerLinePolicy(boolean)
	 */
	public void setOneRecordPerLinePolicy(boolean b) {
		// TODO Auto-generated method stub

	}

	public boolean isSaveIndex() {
		return saveIndex;
	}

	public boolean isAppend() {
		return append;
	}

	public void setAppend(boolean append) {
		this.append = append;
	}

	/**
	 * @param index
	 */

}
