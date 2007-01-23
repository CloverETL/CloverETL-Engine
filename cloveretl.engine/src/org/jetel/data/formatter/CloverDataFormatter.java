
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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
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
 * or to binary files with analogical directory structure
 * 
 * 
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

	
	/**
	 * Constructor
	 * 
	 * @param fileName name of archive or name of binary file with records
	 * @param saveIndex whether to save indexes of records or not 
	 */
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
	 * @see org.jetel.data.formatter.Formatter#init(org.jetel.metadata.DataRecordMetadata)
	 */
	public void init(DataRecordMetadata _metadata)
			throws ComponentNotReadyException {
		this.metadata = _metadata;
        buffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
 	}

    /* (non-Javadoc)
     * @see org.jetel.data.formatter.Formatter#setDataTarget(java.lang.Object)
     */
    public void setDataTarget(Object outputDataTarget) {
        //create output stream
        if (outputDataTarget instanceof ZipOutputStream) {
            this.out = (ZipOutputStream)outputDataTarget;
        }else{
            this.out = (FileOutputStream)outputDataTarget;
        }
        writer = Channels.newChannel(this.out);
        if (saveIndex) {//create temporary index file
            String dataDir = fileURL.substring(0,fileURL.lastIndexOf(File.separatorChar)+1);
            idxTmpFile = new File(dataDir + fileName  + ".idx.tmp");
            try{
                idxWriter = Channels.newChannel(new DataOutputStream(
                        new FileOutputStream(idxTmpFile)));
            }catch(FileNotFoundException ex){
                throw new RuntimeException(ex);
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
				if (idxTmpFile.length() > 0){//if some indexes were saved to tmp file, save the rest of indexes
					ByteBufferUtils.flush(idxBuffer,idxWriter);
					idxWriter.close();
					ByteBufferUtils.reload(idxBuffer,idxReader);
				}
				idxBuffer.flip();
				long startValue = 0;//first index
				int position;
				if (out instanceof ZipOutputStream) {
					((ZipOutputStream)out).closeEntry();
					//put entry INEX/fileName.idx
					((ZipOutputStream)out).putNextEntry(new ZipEntry("INDEX" + File.separator + fileName + ".idx"));
					File index = new File(fileURL + ".tmp");//if append=true exist old indexes
					if (append && index.exists()){
						//rewrite old indexes
						ZipInputStream zipIndex = new ZipInputStream(
								new FileInputStream(index));
						while (!(zipIndex.getNextEntry()).getName().equalsIgnoreCase(
								"INDEX" + File.separator + fileName + ".idx")) {}
						DataInputStream inIndex = new DataInputStream(zipIndex);
						ReadableByteChannel idxChannel = Channels.newChannel(inIndex);
				    	while (ByteBufferUtils.reload(buffer,idxChannel) == buffer.capacity()){
				    		ByteBufferUtils.flush(buffer, writer);
				    	}
				    	//get last old index
				    	startValue = buffer.getLong(buffer.limit() - LONG_SIZE_BYTES);
				    	ByteBufferUtils.flush(buffer, writer);
				    	inIndex.close();
						zipIndex.close();
						index.delete();
					}
					//append indexes from tmp file 
					do {
						startValue = changSizeToIndex(startValue);
						position = buffer.position();
						flush();
					}while (position == buffer.limit());
					//clear up
					idxTmpFile.delete();
					idxTmpFile.getParentFile().delete();
				}else{//out instanceof FileOutputStream
					File idxInFile = new File(idxTmpFile.getCanonicalPath().substring(0,idxTmpFile.getCanonicalPath().lastIndexOf('.')));
			    	//get last old index
					if (append && idxInFile.length() > 0) {
						DataInputStream idxIn = new DataInputStream(
								new FileInputStream(idxInFile));
						idxIn.skip(idxInFile.length() - LONG_SIZE_BYTES);
						startValue = idxIn.readLong();
						idxIn.close();
					}
					//append indexes from tmp file 
					idxWriter = new FileOutputStream(idxInFile,append).getChannel();
					do {
						startValue = changSizeToIndex(startValue);
						position = buffer.position();
						ByteBufferUtils.flush(buffer,idxWriter);
					}while (position == buffer.limit());
					//clear up
					idxTmpFile.delete();
					idxWriter.close();
				}
			}
			if (out instanceof ZipOutputStream) {
				((ZipOutputStream)out).closeEntry();
			}else{
				out.close();
			}
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}
	
	/**
	 * This method fills buffer with indexes created from records's sizes stored
	 *  in tmp file or idxBuffer
	 * 
	 * @param lastValue value to start from
	 * @return
	 * @throws IOException
	 */
	private long changSizeToIndex(long lastValue) throws IOException{
		short actualValue;
		while (buffer.remaining() >= LONG_SIZE_BYTES){
			if (idxBuffer.remaining() < SHORT_SIZE_BYTES ){//end of idxBuffer, reload it
				ByteBufferUtils.reload(idxBuffer,idxReader);
				idxBuffer.flip();
			}
			if (idxBuffer.remaining() < SHORT_SIZE_BYTES ){//there is no more sizes to working up
				break;
			}
			buffer.putLong(lastValue);
			actualValue = idxBuffer.getShort();
			//if negative value change to big Integer
			lastValue += actualValue > 0 ? actualValue : Short.MAX_VALUE - actualValue;
		}
		return lastValue;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#write(org.jetel.data.DataRecord)
	 */
	public int write(DataRecord record) throws IOException {
		recordSize = record.getSizeSerialized();
		if (saveIndex) {
			//if size is grater then Short, change to negative Short
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
        
        return recordSize + LEN_SIZE_SPECIFIER;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.formatter.Formatter#flush()
	 */
	public void flush() throws IOException {
		ByteBufferUtils.flush(buffer,writer);
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
