/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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

// FILE: c:/projects/jetel/org/jetel/graph/Edge.java

package org.jetel.graph;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;

/**
 * A class that represents DirectEdge - data connection between two NODEs.<br>
 * This Edge is in-memory buffered for better performance, however the buffer is limited in number of records
 * it can keep - the size is determined by INTERNAL_BUFFERS_NUM constant.
 *
 * @author     D.Pavlis
 * @see        org.jetel.graph.InputPort
 * @see        org.jetel.graph.OutputPort
 * @revision   $Revision$
 */
public class BufferedEdge extends EdgeBase {

    private final static int DEFAULT_INTERNAL_DATA_BUFFER_SIZE = Defaults.Record.MAX_RECORD_SIZE*10;
	private final static int BUFFER_COMPACT_LIMIT_PERCENT = 85;
	
	protected int recordCounter;
	
	// data buffer which keeps data records written by
	// producer
	private ByteBuffer dataBuffer;
	
	// internal counter - how many records are currently in buffer
	private volatile int noRecs;
	// from which position in the buffer we should do next read
	private int readPosition;
	
	protected boolean isOpen;
	
	// if this position is reached
	// while reading data,  we compact data buffer
	private int compactLimitPosition;
	
	/**
	 * Comment for <code>deadlockTick</code>
	 * Deadlock detector. Gets increased each time watchdog
	 * checks this Edge. Each compleded write or record operation
	 * resets it. If counting reaches certain maximum, it is a signal
	 * that this edge is probably in deadlock state.
	 */
	private volatile int deadlockTick;
	
	// used internally when when reading data from buffer
	private DataRecord dataRecord;

	private int internalBufferSize;
	
	
	
	public BufferedEdge(Edge proxy) {
	    this(proxy,DEFAULT_INTERNAL_DATA_BUFFER_SIZE);
	}

	/**
	 * Constructor for the Edge object
	 * 
	 * @param proxy Edge proxy
	 * @param internalBufferSize	how much space allocate for internal buffer which is
	 * used for storing/buffering data records
	 */
	public BufferedEdge(Edge proxy,int internalBufferSize) {
	    super(proxy);
	    this.internalBufferSize=internalBufferSize;
	}
	
	
	public int getRecordCounter() {
		return recordCounter;
	}


	
	/**
	 *  Gets the Open attribute of the Edge object
	 *
	 * @return    The Open value
	 * @since     June 6, 2002
	 */
	public boolean isOpen() {
		return isOpen;
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  IOException  Description of Exception
	 * @since                   April 2, 2002
	 */
	public void init() throws IOException {
		// initialize & open the data pipe
		// we are ready to supply data
	    dataBuffer=ByteBuffer.allocateDirect(internalBufferSize);
	    compactLimitPosition=(dataBuffer.capacity()*BUFFER_COMPACT_LIMIT_PERCENT)/100;
		dataRecord=new DataRecord(proxy.metadata);
	    dataRecord.init();
		recordCounter = 0;
		noRecs=readPosition=0;
		deadlockTick = 0;
		isOpen=true;
	}


	/**
	 * Increase deadlockTic counter by one.
	 */
	public void tick(){
	    deadlockTick++;
	}

	/**
	 * Get status of deadlockTic counter;
	 * 
	 * @return	deadlockTic counter
	 */
	public int getTick(){
	    return deadlockTick;
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

	public synchronized DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
	    if (!isOpen && noRecs==0){
		    return null;
		}
	    
		while(noRecs==0){
		    wait();
		    if (!isOpen && noRecs==0){
			    return null;
			}
		}
		int tmpPosition=dataBuffer.position();
		dataBuffer.position(readPosition);
		record.deserialize(dataBuffer);
		readPosition=dataBuffer.position();
		dataBuffer.position(tmpPosition);
		noRecs--;
		packBuffer();
		deadlockTick=0;
		notify();
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
	public synchronized boolean readRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
		if (!isOpen && noRecs==0){
		    return false;
		}
	    
		while(noRecs==0){
		    wait();
		    if (!isOpen && noRecs==0){
			    return false;
			}
		}
		int tmpPosition=dataBuffer.position();
		dataBuffer.position(readPosition);
		dataRecord.deserialize(dataBuffer);
		readPosition=dataBuffer.position();
		dataBuffer.position(tmpPosition);
		dataRecord.serialize(record);
		record.flip();
		noRecs--;
		packBuffer();
		deadlockTick=0;
		notify();
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
	public synchronized void writeRecord(DataRecord record) throws IOException, InterruptedException {
	    int size=record.getSizeSerialized();
	    while (size>dataBuffer.remaining()){
	        wait();
		}
	    record.serialize(dataBuffer);
	    deadlockTick=0;
	    recordCounter++;
	    noRecs++;
	    notify();
	}


	/**
	 *  Description of the Method
	 *
	 * @param  record                    Description of Parameter
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            August 13, 2002
	 */
	public synchronized void writeRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
		int size=record.remaining();
	    while (size>dataBuffer.remaining()){
	        wait();
		}
	    dataBuffer.put(record);
	    deadlockTick=0;
	    recordCounter++;
	    noRecs++;
	    notify();
	}


	private final void packBuffer(){
	    int position=dataBuffer.position();
	    
	    if (readPosition==position){
	        dataBuffer.clear();
	        readPosition=0;
	    }else if (readPosition>=compactLimitPosition){
	        int diff=position-readPosition;
	        dataBuffer.position(readPosition);
	        dataBuffer.compact();
	        dataBuffer.position(diff);
	        readPosition=0;
	    }
	}
	

	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public synchronized void open() {
		isOpen=true;
		notify();
	}


	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public synchronized void close() {
		isOpen=false;
		notify();
	}

	public boolean hasData(){
		return (noRecs!=0);
	}

    /**
     * @param internalBufferSize The internalBufferSize to set.
     */
    public void setInternalBufferSize(int internalBufferSize) {
        if (internalBufferSize>DEFAULT_INTERNAL_DATA_BUFFER_SIZE)
            this.internalBufferSize = internalBufferSize;
    }
}
/*
 *  end class DirectEdge
 */

