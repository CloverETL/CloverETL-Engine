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

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.tape.DataRecordTape;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.bytes.CloverBuffer;

/**
 * A class that represents PhaseConnectionEdge - data connection between two NODEs in different Phases.<br>
 * This Edge is in-memory & on disk buffered. It performs a bridge between two Phases of transformation
 * graph.<br>
 * Normal operation is that Node1 starts writing to this Edge. When finished, next Phase (and next Node2) 
 * is started.  Node2 starts reading from this Edge. In generall writing & reading MUST NOT be mixed.
 *
 *
 * @author      D.Pavlis
 * @since       April 2, 2002
 * @see         org.jetel.graph.InputPort
 * @see         org.jetel.graph.OutputPort
 */
public class PhaseConnectionEdge extends EdgeBase {

	private DataRecordTape dataTape;
	private long writeCounter;
	private long readCounter;
    private long writeByteCounter;
    private long readByteCounter;
	private boolean isReadMode;
	private boolean wasInitialized;

	private volatile boolean isEmpty;
	
    /**
     * Monitor for {@link #waitForEOF()}
     */
	private final Object eofMonitor = new Object();
	
	private CloverBuffer recordBuffer;

	/**
	 *Constructor for the PhaseConnectionEdge object
	 *
	 * @param  proxy        Description of the Parameter
	 */
	public PhaseConnectionEdge(Edge proxy) {
		super(proxy);
		dataTape = new DataRecordTape();
		recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		isReadMode=false;
		wasInitialized = false;
		isEmpty = false;
	}

	@Override
	public long getOutputRecordCounter() {
		return writeCounter;
	}

    @Override
	public long getInputRecordCounter() {
        return readCounter;
    }

    @Override
	public long getOutputByteCounter(){
        return writeByteCounter;
    }

    @Override
	public long getInputByteCounter(){
        return readByteCounter;
    }

    @Override
	public int getBufferedRecords() {
        return (int) (writeCounter - readCounter);
    }
    
    @Override
    public int getUsedMemory() {
    	return recordBuffer.capacity() + dataTape.getBufferSize();
    }
    
	/**
	 *  Description of the Method
	 *
	 * @exception  IOException  Description of Exception
	 * @throws InterruptedException 
	 * @since                   April 2, 2002
	 */
	@Override
	public void init() throws IOException, InterruptedException {
		// initialize & open the data pipe
		// we are ready to supply data
		// there are two attemps to initialize this edge
		// first by phase of the writer, then by phase of the reader, we initilize only once
	}

	@Override
	public void preExecute() {
		super.preExecute();

		isReadMode = false;
		isEmpty = false;
		writeCounter = 0;
		readCounter = 0;
        writeByteCounter = 0;
        readByteCounter = 0;
		try {
			if (!wasInitialized) {
				dataTape.open(-1);
				dataTape.addDataChunk();
				wasInitialized = true;
			} else {
				dataTape.clear();
				dataTape.addDataChunk();
			}
		} catch (Exception e) {
			throw new JetelRuntimeException("Phase edge preExecute operation failed.", e);
		}
	}

	// Operations
	/**
	 * An operation that does read one DataRecord from Edge
	 *
	 * @param  record                    Description of Parameter
	 * @return                           Description of the Returned Value
	 * @exception  IOException           Description of Exception
	 * @throws InterruptedException 
	 * @exception  InterruptedException  Description of Exception
	 * @since                            April 2, 2002
	 */
	@Override
	public  DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
	    if (!isReadMode) {
			return null;
		}
		if (dataTape.get(record)){
            readByteCounter+=record.getSizeSerialized();
		    readCounter++;
		    return record;
		} else {
			setEmpty();
		    return null;
		}
		
	}

	/**
	 *  We read data into ByteBuffer directly
	 *
	 * @param  record                    Description of Parameter
	 * @return                           True if success, otherwise false (if no more data)
	 * @exception  IOException           Description of Exception
	 * @throws InterruptedException 
	 * @exception  InterruptedException  Description of Exception
	 * @since                            August 13, 2002
	 */
	@Override
	public boolean readRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
	    if (!isReadMode) {
			return false;
		} 
		
		if (dataTape.get(record)){
            readByteCounter+=record.remaining();
		    readCounter++;
		    return true;
		} else {
			setEmpty();
		    return false;
		}
	}

	/**
	 * An operation that does send one DataRecord through the Edge/PIPE
	 *
	 * @param  record                    Description of Parameter
	 * @exception  IOException           Description of Exception
	 * @throws InterruptedException 
	 * @exception  InterruptedException  Description of Exception
	 * @since                            April 2, 2002
	 */
	@Override
	public void writeRecord(DataRecord record) throws IOException, InterruptedException {
	    if (isReadMode){
		    throw new IOException("Error: Mixed read/write operation on DataRecordTape !");
		}
		recordBuffer.clear();
		record.serialize(recordBuffer);
		recordBuffer.flip();
        writeByteCounter+=recordBuffer.remaining();
		dataTape.put(recordBuffer);
		writeCounter++;
	}

	/**
	 *  We write directly content of ByteBuffer
	 *
	 * @param  record                    Description of Parameter
	 * @exception  IOException           Description of Exception
	 * @throws InterruptedException 
	 * @exception  InterruptedException  Description of Exception
	 * @since                            August 13, 2002
	 */
	@Override
	public  void writeRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
	    if (isReadMode){
		    throw new IOException("Error: Mixed read/write operation on DataRecordTape !");
		}
        writeByteCounter+=record.remaining();
	    dataTape.put(record);
	    writeCounter++;
	}

	/**
	 *  Description of the Method
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws IOException 
	 *
	 * @since    April 2, 2002
	 */
	@Override
	public void eof() throws IOException, InterruptedException {
	    // as writer closes the
	    // port/edge - no more data is to be written, flush
	    // the buffer of the data tape, we will start reading data
        dataTape.flush(false);
        dataTape.rewind();
	    isReadMode=true;
	}

	@Override
	public boolean hasData(){
		return !isEmpty;
	}
	
	/* (non-Javadoc)
	 * clean up
	 * @see java.lang.Object#finalize()
	 */
	@Override
	public void free() {
	    try{
	        dataTape.close();
	    }catch(Exception ex){
	        // ignore
	    }
	}

    @Override
    public boolean isEOF() {
        return isEmpty;
    }
    
    private void setEmpty() {
    	synchronized (eofMonitor) {
    		isEmpty = true;
    		eofMonitor.notifyAll();
    	}
    }
    
    @Override
    public void waitForEOF() throws InterruptedException {
    	synchronized (eofMonitor) {
    		while (!isEmpty) {
    			eofMonitor.wait();
    		}
    	}
    }
    
}
