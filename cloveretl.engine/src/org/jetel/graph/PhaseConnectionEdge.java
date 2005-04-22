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

	private DataRecordTape dataTape;
	private int writeCounter;
	private int readCounter;
	private boolean isOpen;
	private boolean isReadMode;
	private boolean wasInitialized;

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
		if (tmpFilename!=null){
		    dataTape = new DataRecordTape(tmpFilename);
		}else{
		    dataTape = new DataRecordTape();
		}
		recordBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		isReadMode=false;
		wasInitialized = false;
	}



	/**
	 *  Gets the number of records passed through this port IN
	 *
	 * @return    The RecordCounterIn value
	 * @since     April 18, 2002
	 */
	public int getRecordCounter() {
		return isReadMode ? readCounter: writeCounter;
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
		// there are two attemps to initialize this edge
		// first by phase of the writer, then by phase of the reader, we initilize only once
		if (!wasInitialized) {
			writeCounter = readCounter=0;
			dataTape.open();
			dataTape.addDataChunk();
			wasInitialized = true;
			isOpen=true;
		}
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

	public  DataRecord readRecord(DataRecord record) throws IOException {
	    if (! (isOpen && isReadMode)) {
			return null;
		}
		if (dataTape.get(record)){
		    readCounter++;
		    return record;
		}else{
		    return null;
		}
		
	}


	/**
	 *  We read data into ByteBuffer directly
	 *
	 * @param  record                    Description of Parameter
	 * @return                           True if success, otherwise false (if no more data)
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            August 13, 2002
	 */
	public boolean readRecordDirect(ByteBuffer record) throws IOException {
	    if (! (isOpen && isReadMode)) {
			return false;
		} 
		
		if (dataTape.get(record)){
		    readCounter++;
		    return true;
		}else{
		    return false;
		}
	}


	/**
	 * An operation that does send one DataRecord through the Edge/PIPE
	 *
	 * @param  record                    Description of Parameter
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            April 2, 2002
	 */
	public void writeRecord(DataRecord record) throws IOException {
	    if (isReadMode){
		    throw new IOException("Error: Mixed read/write operation on DataRecordTape !");
		}
		recordBuffer.clear();
		record.serialize(recordBuffer);
		recordBuffer.flip();
		dataTape.put(recordBuffer);
		writeCounter++;
	}


	/**
	 *  We write directly content of ByteBuffer
	 *
	 * @param  record                    Description of Parameter
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            August 13, 2002
	 */
	public  void writeRecordDirect(ByteBuffer record) throws IOException {
	    if (isReadMode){
		    throw new IOException("Error: Mixed read/write operation on DataRecordTape !");
		}
	    dataTape.put(record);
	    writeCounter++;
	}


	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public void open() {
		isOpen = true;
	}


	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public void close() {
	    // as writer closes the
	    // port/edge - no more data is to be written, flush
	    // the buffer of the data tape, we will start reading data
	    try {
	        dataTape.flush(false);
	        dataTape.rewind();
		    isReadMode=true;
	    }catch(IOException ex){
	        throw new RuntimeException("Can't flush/rewind DataRecordTape: "+ex.getMessage());
	    }
	}

	public boolean hasData(){
		return (writeCounter > 0 ? true : false);
	}
	
	
	
	
	/* (non-Javadoc)
	 * clean up
	 * @see java.lang.Object#finalize()
	 */
	public void finalize(){
	    try{
	        dataTape.close();
	    }catch(IOException ex){
	        // ignore
	    }
	}
}
/*
 *  end class PhaseConnectionEdge
 */

