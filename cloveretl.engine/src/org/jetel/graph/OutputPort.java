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

// FILE: c:/projects/jetel/org/jetel/OutputPort.java

package org.jetel.graph;
import java.io.IOException;
import org.jetel.data.DataRecord;
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
	public void connectWriter(Node _writer);


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
	 */
	public void close();

	/**
	 * An operation that opens the port indicating that data will be available
	 *
	 * @since    April 2, 2002
	 */
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
	 * @return    The RecordCounter value
	 * @since     May 17, 2002
	 */
	public int getRecordCounter();
}
/*
 *  end interface OutputPort
 */

