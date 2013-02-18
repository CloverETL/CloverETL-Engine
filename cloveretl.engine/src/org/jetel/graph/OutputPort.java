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
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * An interface defining operations expected from OutputPort object
 * Output port establishes unidirectional communication channel between two NODEs.
 * 
 * @author     D.Pavlis
 * @since    April 2, 2002
 * @see     	InputPort
 * @see	       	Node
 * @see		Edge
 */
public interface OutputPort {

	
	// Operations
	/**
	 * An operation that registeres the node which writes data to this port
	 *
	 * @param  _writer  Description of Parameter
	 * @since           April 2, 2002
	 */
	public void connectWriter(Node _writer, int portNum);


	/**
	 * An operation that passes/writes one record through this port
	 *
	 * @param  _record                   Description of Parameter
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            April 2, 2002
	 */
	public void writeRecord(DataRecord _record) throws IOException, InterruptedException;


	/**
	 * An operation that closes the port indicating that no more data is available
	 *
	 * @since    April 2, 2002
     * @deprecated use eof() method instead
	 */
    @Deprecated
	public void close() throws InterruptedException, IOException;

    /**
     * An operation that sends message indicating that no more data is available.
     */
    public void eof() throws InterruptedException, IOException;
    
	/**
	 * An operation that opens the port indicating that data will be available
	 *
	 * @since    April 2, 2002
     * @deprecated
	 */
    @Deprecated
	public void open();

	/**
	 *  Gets the Metadata describing data records passing through this port
	 *
	 * @return    The Metadata value
	 * @since     April 4, 2002
	 */
	public DataRecordMetadata getMetadata();


	/**
	 *  Gets the Reader (Node which reads from this port at the other end - if any)
	 *
	 * @return    The Reader value
	 * @since     May 17, 2002
	 */
	public Node getReader();


	/**
	 *  Gets the number of records passed (so far) through this port
	 *
	 * @return    number of records which passed this port
	 * @since     May 17, 2002
     * @deprecated use getOutputRecordCounter() method instead
	 */
    @Deprecated
	public long getRecordCounter();
    
    /**
     *  Gets the number of records passed (so far) through this output port.
     *
     * @return    number of records which passed this output port
     */
    public long getOutputRecordCounter();
    
    /**
     * Gets the number of bytes passed (so far) through this port
     * 
     * @return  number of bytes which passed this port
     * @deprecated use getOutputByteCounter() method instead
     */
    @Deprecated
    public long getByteCounter();
    
    /**
     * Gets the number of bytes passed (so far) through this output port
     * 
     * @return  number of bytes which passed this output port
     */
    public long getOutputByteCounter();
    
    /**
     * Gets the id number of output port connected writer.
     * @return
     */
    public int getOutputPortNumber();

    /**
	 * Reset port for next graph execution. 
     */
	public void reset() throws ComponentNotReadyException;
	
	/**
	 * @return size of allocated memory on the edge (memory footprint)
	 */
	public int getUsedMemory();

	/**
	 * @return associated edge with this output port
	 */
	public Edge getEdge();

}
/*
 *  end interface OutputPort
 */

