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
package org.jetel.graph;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A class that represents DirectEdge - data connection between two NODEs.<br>
 * This Edge is in-memory buffered for better performance, however the buffer is limited in size 
 * - the size is determined by Defaults.Data.DIRECT_EDGE_INTERNAL_BUFFER_SIZE constant.
 *
 * @author     D.Pavlis
 * @since    	April 2, 2002
 * @see        org.jetel.graph.InputPort
 * @see        org.jetel.graph.OutputPort
 */
public class DirectEdge extends EdgeBase {

	private CloverBuffer readBuffer;
	private CloverBuffer writeBuffer;
	private CloverBuffer tmpDataRecord;
	private long inputRecordCounter;
	private long outputRecordCounter;
    private long byteCounter;
    private AtomicInteger bufferedRecords; 
	private volatile boolean isClosed;
    private boolean readerWait;
    private volatile boolean writerWait;
	private int readBufferLimit;
	
	private final static int EOF=Integer.MAX_VALUE;

    /**
     * Monitor for {@link #waitForEOF()}
     */
	private final Object eofMonitor = new Object();
	
	/**
	 * Timeout (in ms) after which the reading thread forces switch of reading and writing buffers.
	 * @see #forceBuffersSwitch
	 */
	private static final int MAX_WAITING_TIME = 1000;

	/**
	 * This flag indicates, that reading thread is waiting too long.
	 * The reading and writing buffers should be switched as soon as possible.
	 */
	private volatile boolean forceBuffersSwitch;
	
	/**
	 *Constructor for the Edge object
	 *
	 * @param  id        Description of Parameter
	 * @param  metadata  Description of Parameter
	 * @since            April 2, 2002
	 */
	public DirectEdge(Edge proxy) {
		super(proxy);
		
	}
	
	@Override
	public long getOutputRecordCounter() {
		return outputRecordCounter;
	}

    @Override
	public long getInputRecordCounter() {
        return inputRecordCounter;
    }


    @Override
	public long getOutputByteCounter(){
        return byteCounter; 
    }

    @Override
	public long getInputByteCounter(){
        return byteCounter; 
    }

    @Override
	public int getBufferedRecords(){
        return bufferedRecords.get();
    }

    @Override
    public int getUsedMemory() {
    	return writeBuffer.capacity() + readBuffer.capacity() + tmpDataRecord.capacity();
    }
    
	/**
	 *  Description of the Method
	 *
	 * @exception  IOException  Description of Exception
	 * @since                   April 2, 2002
	 */
	@Override
	public void init() throws IOException {
		// initialize & open the data pipe
		// we are ready to supply data
		readBuffer = CloverBuffer.allocateDirect(Defaults.Graph.DIRECT_EDGE_INTERNAL_BUFFER_SIZE);
		writeBuffer = CloverBuffer.allocateDirect(Defaults.Graph.DIRECT_EDGE_INTERNAL_BUFFER_SIZE);
		inputRecordCounter = 0;
		outputRecordCounter = 0;
        byteCounter=0;
        bufferedRecords=new AtomicInteger(0);
		readBuffer.flip(); // we start with empty read buffer
		tmpDataRecord = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		isClosed=false;
	    readerWait=false;
	    writerWait=false;
	}

	@Override
	public void preExecute() {
		super.preExecute();
		
		readBuffer.clear();
		writeBuffer.clear();
		inputRecordCounter = 0;
		outputRecordCounter = 0;
        byteCounter = 0;
        bufferedRecords.set(0);
		readBuffer.flip(); // we start with empty read buffer
		tmpDataRecord.clear();
		isClosed=false;
	    readerWait=false;
	    writerWait=false;
	    forceBuffersSwitch = false;
	}
	
	// Operations
	/**
	 * An operation that does read one DataRecord from Edge
	 *
	 * @param  record                    Description of Parameter
	 * @return                           Description of the Returned Value
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            April 2, 2002
	 */

	@Override
	public DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
	    if (!readBuffer.hasRemaining()) {
	        if (!fillReadBuffer()) {
	            return null;
	        }
	    }
	    try {
	        // create the record/read it from buffer
	        if (ByteBufferUtils.decodeLength(readBuffer) == EOF) {
	        	close();
	            return null; // EOF
	        }
	        record.deserialize(readBuffer);
	    } catch(BufferUnderflowException ex) {
	        throw new IOException("BufferUnderflow when reading/deserializing record. It can be caused by different metadata.");
	    }
        bufferedRecords.decrementAndGet();
        inputRecordCounter++;
        
		return record;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  record                    Description of Parameter
	 * @return                           True if success, otherwise false (if no more data)
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            August 13, 2002
	 */
	@Override
	public boolean readRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
	    
	    if (!readBuffer.hasRemaining()){
	        if (!fillReadBuffer()){
	            return false;
	        }
	    }
	    try{
	        // create the record/read it from buffer
	        int length = ByteBufferUtils.decodeLength(readBuffer);
	        if (length == EOF) {
	        	close();
	            return false;
	        }
	        readBuffer.limit(readBuffer.position()+length);
	        record.clear();
	        record.put(readBuffer);
	        readBuffer.limit(readBufferLimit);
	        record.flip();
	    }catch(BufferUnderflowException ex){
            throw new IOException("BufferUnderflow when reading/deserializing record. It can be caused by different metadata.");
	    }
        bufferedRecords.decrementAndGet();
	    inputRecordCounter++;
	    
	    return true;
	}

	@SuppressFBWarnings("WA_NOT_IN_LOOP")
	private synchronized boolean fillReadBuffer() throws InterruptedException{
	    if(isClosed) return false;
        if(writerWait) {
            switchBuffers();
            writerWait = false;
            notify();
        } else {
            readerWait = true;
    		wait(MAX_WAITING_TIME); //just wait 1 second
    		if (readerWait) { //it seems that 1 second is not enough
    			forceBuffersSwitch = true; //force buffers switch to get at least few records from writing buffer
                while (readerWait) { //wait for writing thread to perform the buffer switch
                	wait();
                }
    		}
        }
	    return true;
	}
	


	/**
	 * An operation that does send one DataRecord through the Edge/PIPE
	 *
	 * @param  record                    Description of Parameter
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            April 2, 2002
	 */
	@Override
	public void writeRecord(DataRecord record) throws IOException, InterruptedException {

        tmpDataRecord.clear();
        try {
            record.serialize(tmpDataRecord);
        } catch (BufferOverflowException ex) {
            throw new IOException(
                    "Internal buffer is not big enough to accomodate data record ! (See RECORD_LIMIT_SIZE parameter)"+
                    "\n [actual record size: "+record.getSizeSerialized()+" bytes]");
        }
        tmpDataRecord.flip();
        int length = tmpDataRecord.remaining();

        if ((length + ByteBufferUtils.SIZEOF_INT) > writeBuffer.remaining() && writeBuffer.position() > 0) {
        	//write buffer is flushed only if serialized record does not fit into write buffer and at least a record is already written
        	//write buffer is not flushed if is empty (even if the written record does not fit into write buffer) - dynamicity of write buffer is used 
            flushWriteBuffer();
        }
        try {
        	ByteBufferUtils.encodeLength(writeBuffer, length);
            writeBuffer.put(tmpDataRecord);
        } catch (BufferOverflowException ex) {
            throw new IOException(
                    "WriteBuffer is not big enough to accomodate data record !");
        }
        // record.serialize(writeBuffer);

        byteCounter += length;

        outputRecordCounter++;
        // one more record written
        bufferedRecords.incrementAndGet();
        
        if (forceBuffersSwitch) {
        	//reading thread forces the buffers switch
        	//we are sure, that writing buffer contains at least one record
        	//so let's switch the buffers
        	flushWriteBuffer();
        }
    }

	/**
	 *  Description of the Method
	 *
	 * @param  record                    Description of Parameter
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            August 13, 2002
	 */
	@Override
	public void writeRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
        int dataLength = record.remaining();

        if ((dataLength + ByteBufferUtils.SIZEOF_INT) > writeBuffer.remaining() && writeBuffer.position() > 0) {
        	//write buffer is flushed only if serialized record does not fit into write buffer and at least a record is already written
        	//write buffer is not flushed if is empty (even if the written record does not fit into write buffer) - dynamicity of write buffer is used 
            flushWriteBuffer();
        }

        try {
        	ByteBufferUtils.encodeLength(writeBuffer, dataLength);
            writeBuffer.put(record);
        } catch (BufferOverflowException ex) {
            throw new IOException(
                    "WriteBuffer is not big enough to accomodate data record ! (See RECORD_LIMIT_SIZE parameter)"+
                    "\n [actual record size: "+record.rewind().remaining()+" bytes]");
        }

        byteCounter += dataLength;
        outputRecordCounter++;
        bufferedRecords.incrementAndGet();
        
        if (forceBuffersSwitch) {
        	//reading thread forces the buffers switch
        	//we are sure, that writing buffer contains at least one record
        	//so let's switch the buffers
        	flushWriteBuffer();
        }
    }

	private synchronized void flushWriteBuffer() throws InterruptedException{
	    if(readerWait) {
	        switchBuffers();
            readerWait = false;
            forceBuffersSwitch = false;
            notify();
        } else {
            writerWait = true;
            while(writerWait) {
    	        wait();
    	    }
        }
	}
	
	private final void switchBuffers(){
		CloverBuffer tmp;
	    tmp=readBuffer;
	    readBuffer=writeBuffer;
	    writeBuffer=tmp;
	    writeBuffer.clear();
	    readBuffer.flip();
	    readBufferLimit=readBuffer.limit(); // save readRecord limit
	}

	/**
	 *  Description of the Method
	 * @throws InterruptedException 
	 *
	 * @since    April 2, 2002
	 */
	@Override
	public void eof() throws InterruptedException {
		if (writeBuffer.remaining() < ByteBufferUtils.SIZEOF_INT) {
            flushWriteBuffer();
        }
        ByteBufferUtils.encodeLength(writeBuffer, EOF); // send EOF
        eofSent = true;
        
        flushWriteBuffer();
	}

    @Override
    public void free() {
        //do nothing
    }
    
	@Override
	public boolean hasData() {
        if(isClosed) return false;
        
        if (!readBuffer.hasRemaining()){
            if (writerWait){
                try {
                    fillReadBuffer();
                } catch (InterruptedException e) {
                    //do nothing, just return
                    return false;
                }
            }else{
                return false;
            }
        }
        return true;
	}

    @Override
    public boolean isEOF() {
        return isClosed;
    }
    
    private void close() {
    	synchronized (eofMonitor) {
    		isClosed = true;
    		eofMonitor.notifyAll();
    	}
    }
    
    @Override
    public void waitForEOF() throws InterruptedException {
    	synchronized (eofMonitor) {
    		while (!isClosed) {
    			eofMonitor.wait();
    		}
    	}
    }
    
}
/*
 *  end class DirectEdge
 */

