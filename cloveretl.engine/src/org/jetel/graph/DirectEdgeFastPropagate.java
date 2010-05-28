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
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;

/**
 * A class that represents DirectEdge - data connection between two NODEs.<br>
 * This Edge is in-memory buffered for better performance, however the buffer is limited in number of records
 * it can keep - the size is determined by INTERNAL_BUFFERS_NUM constant.
 * Note: alternative implementation of direct edge for fast record propagate to reader component.
 * 
 * @author     D.Pavlis
 * @since       April 2, 2002
 * @see        org.jetel.graph.InputPort
 * @see        org.jetel.graph.OutputPort
 * @revision   $Revision: 337 $
 */
public class DirectEdgeFastPropagate extends EdgeBase {

    
    protected EdgeRecordBufferPool recordBuffer;
    protected int recordCounter;
    protected long byteCounter;
    protected AtomicInteger bufferedRecords;

    // Attributes
    
    /**
     *Constructor for the Edge object
     *
     * @param  id        Description of Parameter
     * @param  metadata  Description of Parameter
     * @since            April 2, 2002
     */
    public DirectEdgeFastPropagate(Edge proxy) {
        super(proxy);
        
    }

    public int getOutputRecordCounter() {
        return recordCounter;
    }

    public int getInputRecordCounter() {
        return recordCounter;
    }

    public long getOutputByteCounter() {
        return byteCounter;
    }

    public long getInputByteCounter() {
        return byteCounter;
    }
    
    public int getBufferedRecords(){
        return bufferedRecords.get();
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
        recordBuffer=new EdgeRecordBufferPool(Defaults.Graph.DIRECT_EDGE_FAST_PROPAGATE_NUM_INTERNAL_BUFFERS, 
                                            Defaults.Record.MAX_RECORD_SIZE);
        recordCounter = 0;
        byteCounter=0;
        bufferedRecords=new AtomicInteger(0);

        recordBuffer.open();
    }

    @Override
    public void reset() {
        recordBuffer.reset();
		recordCounter = 0;
		byteCounter=0;
		bufferedRecords.set(0);
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
        ByteBuffer buffer;
        // is the port still OPEN ?? - should be as long as the graph executes

        buffer = recordBuffer.getFullBuffer();
        if (buffer == null) {
            return null;
            // no more data in a flow
        }
        // create the record/read it from buffer
        record.deserialize(buffer);
        
        recordBuffer.setFree(buffer);
        bufferedRecords.decrementAndGet();
        
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
        ByteBuffer buffer;

        buffer = recordBuffer.getFullBuffer();
        if (buffer == null) {
            return false;
            // no more data in flow
        }
        record.clear();
        record.put(buffer);
        // copy content of buffer into our record
        recordBuffer.setFree(buffer);
        // free the buffer
        record.flip();
        
        bufferedRecords.decrementAndGet();
        
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
        ByteBuffer buffer;

        buffer = recordBuffer.getFreeBuffer();
        if (buffer == null) {
            throw new IOException("Output port closed !");
        }
        buffer.clear();
        record.serialize(buffer);   // serialize the record
        buffer.flip();
        
        byteCounter+=buffer.remaining();        
        recordBuffer.setFull(buffer);      
        recordCounter++;
        bufferedRecords.incrementAndGet();
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
        ByteBuffer buffer;

        buffer = recordBuffer.getFreeBuffer();
        if (buffer == null) {
            throw new IOException("Output port closed !");
        }
        buffer.clear();
        buffer.put(record);
        buffer.flip();
        
        byteCounter+=buffer.remaining();
        
        recordBuffer.setFull(buffer);
        record.rewind();
        recordCounter++;
        bufferedRecords.incrementAndGet();
    }

    @Override
    public void eof() {
        recordBuffer.close();
    }

    @Override
    public boolean isEOF() {
        return !recordBuffer.isOpen();
    }
    
    @Override
    public void free() {
        //do nothing
    }
    
    public boolean hasData(){
        return recordBuffer.hasData();
    }

    /**
     *  Class implementing semafor/internal buffer for handling
     * Producer,Consumer scenario of two threads.
     *
     * @author     dpavlis
     * @since    June 5, 2002
     */
    static class EdgeRecordBufferPool {
        private final static int MIN_NUM_BUFFERS = 2; // minimum number of internal buffers for correct behaviour
        
        ByteBuffer buffers[];
        volatile int readPointer;
        volatile int writePointer;
        final int size;
        volatile boolean isOpen;
        volatile boolean eofWasRead;


        /**
         *Constructor for the EdgeRecordBufferPool object
         *
         * @param  numBuffers  number of internal buffers allocated for storing DataRecords
         * @param  bufferSize  size of 1 internal buffer - should be similar to org.jetel.data.Defaults.Record.MAX_RECORD_SIZE
         * @since              June 5, 2002
         */
        EdgeRecordBufferPool(int numBuffers, int bufferSize) {
            size= numBuffers > MIN_NUM_BUFFERS ? numBuffers : MIN_NUM_BUFFERS;
            // create/allocate  buffers
            buffers = new ByteBuffer[size];
            readPointer=0;
            writePointer=0;
            for (int i = 0; i < size; i++) {
                buffers[i] = ByteBuffer.allocateDirect(bufferSize);
                if (buffers[i] == null) {
                    throw new RuntimeException("Failed buffer allocation");
                }
            }
            isOpen = true; // the buffer is implicitly open - can be read/written
            eofWasRead = false;
        }


        /**
         *  Marks buffer as free for writing
         *
         * @param  buffer  The new Free value
         * @since          June 5, 2002
         */
        synchronized void setFree(ByteBuffer buffer) {
            readPointer=(readPointer+1)%size;
            notify();
        }


        /**
         *  Marks buffer as full/containing data for reading
         *
         * @param  buffer  The new Full value
         * @since          June 5, 2002
         */
        synchronized void setFull(ByteBuffer buffer) {
            writePointer=(writePointer+1)%size;
            notify();
        }


        /**
         * Determines status of this buffer pool (open/closed) 
         *
         * @return    True if EdgeRecordBufferPool is open for reading&writing
         * @since     June 6, 2002
         */
        synchronized boolean isOpen() {
            return isOpen;
        }


        /**
         *  Gets buffer which can be used for writing record or
         * waits till such buffer exists (is freed).
         * If EdgeRecordBufer is closed, it returns null.
         *
         * @return                           The EmptyBuffer value
         * @exception  InterruptedException  Description of Exception
         * @since                            June 5, 2002
         */
        synchronized ByteBuffer getFreeBuffer() throws InterruptedException {
            // if already closed - return null - NoMoreData required
            if (!isOpen){
                return null;
            }
            // can we move forward in next step ?
            int tmpWrite=(writePointer+1)%size;
            while(tmpWrite==readPointer){
                // the next slot is still occupied by read thread
                wait();
                if (!isOpen) {
                    return null;
                }
            }
            return buffers[writePointer];
        }


        /**
         *  Gets one buffer containing data or waits till some buffer is filled by
         * producer.
         *
         * @return                           The FullBuffer value
         * @exception  InterruptedException  Description of Exception
         * @since                            June 5, 2002
         */
        synchronized ByteBuffer getFullBuffer() throws InterruptedException {
            // already closed and no more data left
            if ((!isOpen) && (readPointer==writePointer)) {
            	eofWasRead = true;
                return null;
            }
            while (readPointer==writePointer) {
                // wait till something shows up
                wait();
                if ((!isOpen) && (readPointer==writePointer)){
                	eofWasRead = true;
                    return null;
                }
            }
            return buffers[readPointer];
        }


        /**
         *  Sets end-of-data flag, closes this pool
         *
         * @since    June 5, 2002
         */
        synchronized void close() {
            isOpen = false;
            notify();
        }


        /**
         *  Opens this buffer pool for reading/writing
         *
         * @since    June 6, 2002
         */
        synchronized void open() {
            isOpen = true;
            notify();
        }

        synchronized void reset() {
            readPointer=0;
            writePointer=0;
        	eofWasRead = false;
        	
        	open();
        }
        
        public boolean hasData() {
            return readPointer != writePointer || (!isOpen && !eofWasRead);
        }
    }
}
/*
 *  end class DirectEdge
 */

