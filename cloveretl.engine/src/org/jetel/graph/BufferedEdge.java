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
import org.jetel.data.FileRecordBuffer;

/**
 * A class that represents BufferedEdge - data connection between two NODEs in different Phases.<br>
 * This Edge is in-memory & on disk buffered. It performs a bridge between two Phases of transformation
 * graph.<br>
 * Normal operation is that Node1 starts writing to this Edge. When finished, next Phase (and next Node2) 
 * is started.  Node2 starts reading from this Edge. Although in generall writing & reading can be mixed,
 * it was not meant to work in this way and thus not optimized to perform it efficiently.
 *
 *
 * @author      D.Pavlis
 * @since       April 2, 2002
 * @revision    $Revision$
 * @see         org.jetel.graph.InputPort
 * @see         org.jetel.graph.OutputPort
 */
public class BufferedEdge extends EdgeBase {

	private FileRecordBuffer fileRecordBuffer;
	private int recordCounter;
	private boolean isOpen;// indicates whether we can read from it.
	private boolean wasInitialized;

	private ByteBuffer recordBuffer;
	/**
	 *  Number of internal buffers for storing records
	 *
	 * @since    April 11, 2002
	 */
	private final static int INTERNAL_BUFFERS_NUM = 4;

	private final static int SIZE_OF_DATA_BUFFER = Defaults.Record.MAX_RECORD_SIZE * 32;


	/**
	 *Constructor for the Edge object
	 *
	 * @param  proxy     Description of the Parameter
	 * @since            April 2, 2002
	 */
	public BufferedEdge(Edge proxy) {
		super(proxy);
		fileRecordBuffer = new FileRecordBuffer(null, SIZE_OF_DATA_BUFFER);
		recordBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		isOpen = true;
		wasInitialized = false;
	}


	/**
	 *Constructor for the BufferedEdge object
	 *
	 * @param  proxy        Description of the Parameter
	 * @param  tmpFilename  Description of the Parameter
	 */
	public BufferedEdge(Edge proxy, String tmpFilename) {
		super(proxy);
		fileRecordBuffer = new FileRecordBuffer(tmpFilename, SIZE_OF_DATA_BUFFER);
		recordBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		isOpen = true;
		wasInitialized = false;
	}



	/**
	 *  Gets the number of records passed through this port IN
	 *
	 * @return    The RecordCounterIn value
	 * @since     April 18, 2002
	 */
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
		// there are two attemps to initialize this edge
		// first by phase of the writer, then by phase of the reader, we initilize only once
		if (!wasInitialized) {
			recordCounter = 0;
			fileRecordBuffer.clear();// for safety-sake
			wasInitialized = true;
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

	public synchronized DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
		if (!isOpen) {
			return null;
		}
		ByteBuffer tmpBuffer;
		recordBuffer.clear();
		tmpBuffer = fileRecordBuffer.shift(recordBuffer);
		recordBuffer.flip();
		if (tmpBuffer != null) {
			record.deserialize(recordBuffer);
			return record;
		} else {
			isOpen = false;
			fileRecordBuffer.close(); //force deletion of tmp file (shoud be done automatically but it isn't)
			return null;
		}

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
		boolean result;
		if (!isOpen) {
			return false;
		} else {
			if (fileRecordBuffer.shift(record) == null) {
				isOpen = false;
				fileRecordBuffer.close();//force deletion of tmp file (shoud be done automatically but it isn't)
				return false;
			} else {
				return true;
			}
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
	public synchronized void writeRecord(DataRecord record) throws IOException, InterruptedException {
		recordBuffer.clear();
		record.serialize(recordBuffer);
		recordBuffer.flip();
		fileRecordBuffer.push(recordBuffer);
		recordCounter++;// one more record written
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
		fileRecordBuffer.push(record);
		recordCounter++;
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
		// not used but could indicate, that no more records
		// will be written
	}

	public boolean hasData(){
		return (fileRecordBuffer.isEmpty() ? false : true);
	}
}
/*
 *  end class BufferedEdge
 */

