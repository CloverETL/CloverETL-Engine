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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;

/**
 * A class that represents DirectEdge - data connection between two NODEs.<br>
 * This Edge is in-memory buffered for better performance, however the buffer is limited in size 
 * - the size is determined by Defaults.Data.DIRECT_EDGE_INTERNAL_BUFFER_SIZE constant.
 *
 * @author     D.Pavlis
 * @since    	April 2, 2002
 * @see        org.jetel.graph.InputPort
 * @see        org.jetel.graph.OutputPort
 * @revision   $Revision$
 */
public class DirectEdge extends EdgeBase {

	
	private ByteBuffer readBuffer;
	private ByteBuffer writeBuffer;
	private ByteBuffer tmpDataRecord;
	private int recordCounter;
	private boolean isClosed=false;
	private boolean readFull=false;
	
	private int readBufferLimit;

	// Attributes
	

	private final static int EOF=Integer.MIN_VALUE;
	private	final static int SIZEOF_INT=4;	

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
		return !isClosed;
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
		readBuffer= ByteBuffer.allocateDirect(Defaults.Graph.DIRECT_EDGE_INTERNAL_BUFFER_SIZE);
		writeBuffer=ByteBuffer.allocateDirect(Defaults.Graph.DIRECT_EDGE_INTERNAL_BUFFER_SIZE);
		recordCounter = 0;
		readBuffer.flip(); // we start with empty read buffer
		tmpDataRecord=ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
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

	public DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
	    
	    if (!readBuffer.hasRemaining()){
	        if (!fillReadBuffer()){
	            return null;
	        }
	    }
	    try{
	        // create the record/read it from buffer
	        if (readBuffer.getInt()<0){
	            isClosed=true;
	            return null; // EOF
	        }
	        record.deserialize(readBuffer);
	    }catch(BufferUnderflowException ex){
	        throw new IOException(ex.getMessage());
	    }
	    
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
	public boolean readRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
	    
	    if (!readBuffer.hasRemaining()){
	        if (!fillReadBuffer()){
	            return false;
	        }
	    }
	    try{
	        // create the record/read it from buffer
	        int length=readBuffer.getInt();
	        if (length<0){
	            isClosed=true;
	            return false;
	        }
	        readBuffer.limit(readBuffer.position()+length);
	        record.clear();
	        record.put(readBuffer);
	        readBuffer.limit(readBufferLimit);
	        record.flip();
	    }catch(BufferUnderflowException ex){
	        throw new IOException(ex.getMessage());
	    }
	    
	    
	    return true;
	}

	
	private synchronized boolean fillReadBuffer() throws InterruptedException{
	    if(isClosed) return false;
	    readFull=false;
	    while (!readFull){
	        notify();
	        wait();
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
	public void writeRecord(DataRecord record) throws IOException, InterruptedException {
		
	    tmpDataRecord.clear();
	    record.serialize(tmpDataRecord);
	    tmpDataRecord.flip();
	    int length=tmpDataRecord.remaining();
	    
	    if ((length+SIZEOF_INT)>writeBuffer.remaining()){
	        flushWriteBuffer();
	    }
        writeBuffer.putInt(length);
        writeBuffer.put(tmpDataRecord);
        // record.serialize(writeBuffer);

        recordCounter++;
		// one more record written
	}

	/**
	 *  Description of the Method
	 *
	 * @param  record                    Description of Parameter
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            August 13, 2002
	 */
	public void writeRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
		int dataLength=record.remaining();
	    
	    if ((dataLength+SIZEOF_INT)>writeBuffer.remaining()){
	        flushWriteBuffer();
	    }
        writeBuffer.putInt(dataLength);
        writeBuffer.put(record);

        recordCounter++;
	    
	}

	private synchronized void flushWriteBuffer() throws InterruptedException{
	    while(readFull){
	        notify();
	        wait();
	    }
	    // just switch buffers
	    switchBuffers();

	    readFull=true;
	    notify();
	}
	
	private final void switchBuffers(){
	    ByteBuffer tmp;
	    tmp=readBuffer;
	    readBuffer=writeBuffer;
	    writeBuffer=tmp;
	    writeBuffer.clear();
	    readBuffer.flip();
	    readBufferLimit=readBuffer.limit(); // save readRecord limit
	}

	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public void open() {
		isClosed=false;
		readBuffer.clear();
		readBuffer.flip();
		writeBuffer.clear();
	}


	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public synchronized void close() {
	    try{
	        if (writeBuffer.remaining()<SIZEOF_INT){
	            flushWriteBuffer();
	        }
            writeBuffer.putInt(EOF); // send EOF
            flushWriteBuffer();
            
	    }catch(InterruptedException ex){
	        throw new RuntimeException(ex.getClass().getName()+":"+ex.getMessage());
	    }
	}

	public boolean hasData(){
	    return readBuffer.hasRemaining();
	}
}
/*
 *  end class DirectEdge
 */

