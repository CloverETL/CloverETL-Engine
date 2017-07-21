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
 * An interface defining operations expected from InputPort object.
 * Input port establishes communication channel between two NODEs.
 *
 * @author     	D.Pavlis
 * @since    	April 2, 2002
 * @see        	OutputPort
 * @see	       	Node
 * @see		Edge
 */
public interface InputPort {

	
	// Operations
	/**
	 * An operation that registeres the node which reads data from this port
	 *
	 * @param  _reader  Description of Parameter
	 * @since           April 2, 2002
	 */
	public void connectReader(Node _reader, int portNum);


	/**
	 *An operation that reads one record from this port
	 *
	 * @param  record                    Description of Parameter
	 * @return                           Description of the Returned Value
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            April 2, 2002
	 */
	public DataRecord readRecord(DataRecord record) throws IOException, InterruptedException;


	/**
	 * An operation that checks whether port is open for reading
	 *
	 * @return    The EOF value
	 * @since     April 2, 2002
     * @deprecated
	 */
    @Deprecated
	public boolean isOpen();

    /**
     * This method tests whether EOF mark was sended via this port.
     * @return
     */
    public boolean isEOF();

	/**
	 *  Gets the Metadata describing data records passing through this port
	 *
	 * @return    The Metadata value
	 * @since     April 4, 2002
	 */
	public DataRecordMetadata getMetadata();


	/**
	 *  Gets the Writer (Node which writes into this port at the other end - if any)
	 *
	 * @return    The Writer value
	 * @since     May 17, 2002
	 */
	public Node getWriter();


	/**
	 *  Gets the number of records passed (so far) through this port
	 *
	 * @return    number of records which passed this port
	 * @since     May 17, 2002
     * @deprecated use getInputRecordCounter() method instead
	 */
    @Deprecated
	public long getRecordCounter();
    
    /**
     *  Gets the number of records passed (so far) through this input port.
     *
     * @return    number of records which passed this input port
     */
    public long getInputRecordCounter();
    
    /**
     * Gets the number of bytes passed (so far) through this port
     * 
     * @return  number of bytes which passed this port
     * @deprecated use getInputByteCounter() method instead
     */
    @Deprecated
    public long getByteCounter();

    /**
     * Gets the number of bytes passed (so far) through this input port.
     * 
     * @return  number of bytes which passed this input port
     */
    public long getInputByteCounter();
	
	/**
	 * Method which tests whether data (including EOF flag) is awaiting/ready to be read.
	 * Following read operation should not block, nonetheless can return null as EOF flag.
	 * 
	 * @return True if read operation won't block due to lack of data
	 */
	public boolean hasData();
    
    /**
     * Gets the id number of input port connected reader.
     * @return
     */
    public int getInputPortNumber();

    /**
	 * Reset port for next graph execution. 
     */
	public void reset() throws ComponentNotReadyException;

	/**
	 * @return size of allocated memory on the edge (memory footprint)
	 */
	public int getUsedMemory();
	
	/**
	 * @return associated edge with this input port
	 */
	public Edge getEdge();
	
}
/*
 *  end interface InputPort
 */

