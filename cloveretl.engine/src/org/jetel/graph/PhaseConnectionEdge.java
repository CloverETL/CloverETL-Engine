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
import org.jetel.data.tape.DataRecordTape;

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
 * @revision    $Revision$
 * @see         org.jetel.graph.InputPort
 * @see         org.jetel.graph.OutputPort
 */
public class PhaseConnectionEdge extends EdgeBase {

	private String tmpFilename;
	private DataRecordTape dataTape;
	private int writeCounter;
	private int readCounter;
    private long writeByteCounter;
    private long readByteCounter;
	private boolean isReadMode;
	private boolean wasInitialized;

	private boolean isEmpty;
	
	private ByteBuffer recordBuffer;
	
	/**
	 *Constructor for the Edge object
	 *
	 * @param  proxy     Description of the Parameter
	 * @since            April 2, 2002
	 */
	public PhaseConnectionEdge(Edge proxy) {
		this(proxy,null);
	}


	/**
	 *Constructor for the PhaseConnectionEdge object
	 *
	 * @param  proxy        Description of the Parameter
	 * @param  tmpFilename  Description of the Parameter
	 */
	public PhaseConnectionEdge(Edge proxy, String tmpFilename) {
		super(proxy);
		this.tmpFilename = tmpFilename;
		if (tmpFilename!=null){
		    dataTape = new DataRecordTape(tmpFilename);
		}else{
		    dataTape = new DataRecordTape();
		}
		recordBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		isReadMode=false;
		wasInitialized = false;
		isEmpty = false;
	}



	public int getOutputRecordCounter() {
		return writeCounter;
	}

    public int getInputRecordCounter() {
        return readCounter;
    }

    public long getOutputByteCounter(){
        return writeByteCounter;
    }

    public long getInputByteCounter(){
        return readByteCounter;
    }

    public int getBufferedRecords(){
        return writeCounter-readCounter;
    }
    
	/**
	 *  Description of the Method
	 *
	 * @exception  IOException  Description of Exception
	 * @throws InterruptedException 
	 * @since                   April 2, 2002
	 */
	public void init() throws IOException, InterruptedException {
		// initialize & open the data pipe
		// we are ready to supply data
		// there are two attemps to initialize this edge
		// first by phase of the writer, then by phase of the reader, we initilize only once
		if (!wasInitialized) {
			writeCounter = readCounter=0;
            writeByteCounter=readByteCounter=0;
			dataTape.open();
			dataTape.addDataChunk();
			wasInitialized = true;
		}
	}

	@Override
	public void reset() {
		isReadMode=false;
		isEmpty = false;
		writeCounter = 0;
		readCounter = 0;
        writeByteCounter = 0;
        readByteCounter = 0;
		try {
			dataTape.clear();
			dataTape.addDataChunk();
		} catch (Exception e) {
			if (tmpFilename!=null){
			    dataTape = new DataRecordTape(tmpFilename);
			}else{
			    dataTape = new DataRecordTape();
			}
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

	public  DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
	    if (!isReadMode) {
			return null;
		}
		if (dataTape.get(record)){
            readByteCounter+=record.getSizeSerialized();
		    readCounter++;
		    return record;
		}else{
			isEmpty = true;
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
	public boolean readRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
	    if (!isReadMode) {
			return false;
		} 
		
		if (dataTape.get(record)){
            readByteCounter+=record.remaining();
		    readCounter++;
		    return true;
		}else{
			isEmpty = true;
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
	public  void writeRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
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
	public void eof() throws IOException, InterruptedException {
	    // as writer closes the
	    // port/edge - no more data is to be written, flush
	    // the buffer of the data tape, we will start reading data
        dataTape.flush(false);
        dataTape.rewind();
	    isReadMode=true;
	}

	public boolean hasData(){
		return !isEmpty;
//		return writeCounter - readCounter > 0;
	}
	
	/* (non-Javadoc)
	 * clean up
	 * @see java.lang.Object#finalize()
	 */
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
    
}
