/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002  David Pavlis
*
*    This program is free software; you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation; either version 2 of the License, or
*    (at your option) any later version.
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

// FILE: c:/projects/jetel/org/jetel/graph/Edge.java

package org.jetel.graph;
import java.util.*;
import java.io.*;
import java.nio.*;
import org.jetel.data.*;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.graph.InputPort;
import org.jetel.graph.OutputPort;

/**
 * A class that represents Edge - data connection between two NODEs
 *
 * @author     D.Pavlis
 * @since    April 2, 2002
 * @see        org.jetel.graph.InputPort
 * @see        org.jetel.graph.OutputPort
 * @revision   $Revision$
 */
public class Edge implements InputPort, OutputPort, InputPortDirect, OutputPortDirect {

	// Associations
	/**
	 * @since    April 2, 2002
	 */
	protected TransformationGraph graph;

	/**
	 * An attribute that represents ...
	 *
	 * @since    April 2, 2002
	 */
	// protected DataRecord recordBuffer;
	/**
	 * An attribute that represents ...
	 *
	 * @since    April 2, 2002
	 */
	protected String id;
	/**
	 * An attribute that represents ...
	 *
	 * @since    April 2, 2002
	 */
	protected Node reader;
	/**
	 * An attribute that represents ...
	 *
	 * @since    April 2, 2002
	 */
	protected Node writer;
	/**
	 * An attribute that represents ...
	 *
	 * @since    April 2, 2002
	 */
	protected DataRecordMetadata metadata;

	/**
	 *  Records are written to this channel by WriteRecord operation
	 *
	 * @since    April 11, 2002
	 */

	/**
	 *  Description of the Field
	 *
	 * @since    April 11, 2002
	 */
	protected EdgeRecordBuffer recordBuffer;

	/**
	 *  Description of the Field
	 *
	 * @since    April 18, 2002
	 */
	protected int recordCounter;

	// Attributes
	
	/**
	 *  Number of internal buffers for storing records
	 *
	 * @since    April 11, 2002
	 */
	private final static int INTERNAL_BUFFERS_NUM = 4;


	/**
	 *Constructor for the Edge object
	 *
	 * @param  id        Description of Parameter
	 * @param  metadata  Description of Parameter
	 * @since            April 2, 2002
	 */
	public Edge(String id, DataRecordMetadata metadata) {
		this.id = new String(id);
		this.metadata = metadata;
		this.graph = null;
		recordBuffer = new EdgeRecordBuffer(INTERNAL_BUFFERS_NUM, Defaults.Record.MAX_RECORD_SIZE);
		reader = writer = null;
		recordCounter = 0;
	}


	/**
	 *  Sets the Graph attribute of the Edge object
	 *
	 * @param  graph  The new Graph value
	 * @since         April 11, 2002
	 */
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}


	/**
	 * An operation that does ...
	 *
	 * @return    The ID value
	 * @since     April 2, 2002
	 */
	public String getID() {
		return id;
	}


	/**
	 *  Gets the Metadata attribute of the Edge object
	 *
	 * @return    The Metadata value
	 * @since     April 4, 2002
	 */
	public DataRecordMetadata getMetadata() {
		return metadata;
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
	 *  Gets the Reader attribute of the Edge object
	 *
	 * @return    The Reader value
	 * @since     May 21, 2002
	 */
	public Node getReader() {
		return reader;
	}


	/**
	 *  Gets the Writer attribute of the Edge object
	 *
	 * @return    The Writer value
	 * @since     May 21, 2002
	 */
	public Node getWriter() {
		return writer;
	}


	/**
	 *  Gets the Open attribute of the Edge object
	 *
	 * @return    The Open value
	 * @since     June 6, 2002
	 */
	public boolean isOpen() {
		return recordBuffer.isOpen();
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
		recordCounter = 0;
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

		//System.out.println("Going to call getFullBuffer - edge :"+id);
		buffer = recordBuffer.getFullBuffer();
		if (buffer == null) {
			return null;
			// no more data in a flow
		}
		//System.out.println("Going to deserialize  - edge :"+id);
		record.deserialize(buffer);
		// create the record/read it from buffer
		//System.out.println("Going to call setFree - edge :"+id);
		recordBuffer.setFree(buffer);

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

		//System.out.println("Going to call getEmptyBuffer - edge :"+id);
		buffer = recordBuffer.getFreeBuffer();
		if (buffer == null) {
			throw new IOException("Output port closed !");
		}
		//System.out.println("Going to call serialize - edge :"+id);
		buffer.clear();
		record.serialize(buffer);
		// serialize the record
		buffer.flip();
		//System.out.println("Going to call setFull - edge :"+id);
		recordBuffer.setFull(buffer);

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
		ByteBuffer buffer;

		//System.out.println("Going to call getEmptyBuffer - edge :"+id);
		buffer = recordBuffer.getFreeBuffer();
		if (buffer == null) {
			throw new IOException("Output port closed !");
		}
		buffer.clear();
		buffer.put(record);
		buffer.flip();
		recordBuffer.setFull(buffer);
		recordCounter++;
	}


	/**
	 * An operation that does ...
	 *
	 * @param  _reader  Description of Parameter
	 * @since           April 2, 2002
	 */
	public void connectReader(Node _reader) {
		this.reader = _reader;
	}


	/**
	 * An operation that does ...
	 *
	 * @param  _writer  Description of Parameter
	 * @since           April 2, 2002
	 */
	public void connectWriter(Node _writer) {
		this.writer = _writer;
	}


	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public void open() {
		recordBuffer.open();
	}


	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public void close() {
		recordBuffer.close();
	}


	/**
	 *  Description of the Class
	 *
	 * @author     dpavlis
	 * @since    June 5, 2002
	 */
	class EdgeRecordBuffer {
		ByteBuffer buffers[];
		LinkedList free;
		LinkedList full;
		boolean isOpen;


		/**
		 *Constructor for the EdgeRecordBuffer object
		 *
		 * @param  numBuffers  Description of Parameter
		 * @param  bufferSize  Description of Parameter
		 * @since              June 5, 2002
		 */
		EdgeRecordBuffer(int numBuffers, int bufferSize) {
			// create buffers
			buffers = new ByteBuffer[numBuffers];
			free = new LinkedList();
			full = new LinkedList();
			for (int i = 0; i < numBuffers; i++) {
				buffers[i] = ByteBuffer.allocateDirect(bufferSize);
				if (buffers[i] == null) {
					throw new RuntimeException("Failed buffer allocation");
				}
				free.addLast(buffers[i]);
			}
			isOpen = true;
			// the buffer is implicitly open - can be read/written
		}


		/**
		 *  Marks buffer as free for writing
		 *
		 * @param  buffer  The new Free value
		 * @since          June 5, 2002
		 */
		synchronized void setFree(ByteBuffer buffer) {
			free.addLast(buffer);
			notify();
		}


		/**
		 *  Marks buffer as full/containing data for reading
		 *
		 * @param  buffer  The new Full value
		 * @since          June 5, 2002
		 */
		synchronized void setFull(ByteBuffer buffer) {
			full.addLast(buffer);
			notify();
		}


		/**
		 *  Gets the Open attribute of the EdgeRecordBuffer object
		 *
		 * @return    The Open value
		 * @since     June 6, 2002
		 */
		synchronized boolean isOpen() {
			return isOpen;
		}


		/**
		 *  Gets one free buffer or waits till some buffer is free
		 *
		 * @return                           The EmptyBuffer value
		 * @exception  InterruptedException  Description of Exception
		 * @since                            June 5, 2002
		 */
		synchronized ByteBuffer getFreeBuffer() throws InterruptedException {
			// if already closed - return null - EnfOfData
			if ((!isOpen) && (free.isEmpty())) {
				return null;
			}
			while (free.isEmpty()) {
				// while empty, wait
				wait();
				// if still empty & is closed - no more data
				if ((!isOpen) && (free.isEmpty())) {
					return null;
				}
			}
			return (ByteBuffer) free.removeFirst();
		}


		/**
		 *  Gets one buffer containing data or waits till some buffer is filled
		 *
		 * @return                           The FullBuffer value
		 * @exception  InterruptedException  Description of Exception
		 * @since                            June 5, 2002
		 */
		synchronized ByteBuffer getFullBuffer() throws InterruptedException {
			// already closed ?
			if ((!isOpen) && (full.isEmpty())) {
				return null;
			}
			while (full.isEmpty()) {
				// wait till something shows up
				wait();
				if ((!isOpen) && (full.isEmpty())) {
					return null;
				}
			}
			return (ByteBuffer) full.removeFirst();
		}


		/**
		 *  Sets end-of-data flag
		 *
		 * @since    June 5, 2002
		 */
		synchronized void close() {
			isOpen = false;
			notify();
		}


		/**
		 *  Opens all internal buffers for reading/writing
		 *
		 * @since    June 6, 2002
		 */
		synchronized void open() {
			isOpen = true;
			notify();
		}

	}
}
/*
 *  end class Edge
 */

