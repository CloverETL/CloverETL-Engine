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

/**
 *  A class that represents Edge - data connection between two NODEs
 *
 *@author      D.Pavlis
 *@created     4. srpen 2003
 *@since       April 2, 2002
 *@see         org.jetel.graph.InputPort
 *@see         org.jetel.graph.OutputPort
 *@revision    $Revision$
 */
public abstract class EdgeBase {

	private Edge proxy;


	/**
	 *  Constructor for the Edge object
	 *
	 *@param  proxy     Description of the Parameter
	 *@since            April 2, 2002
	 */
	public EdgeBase(Edge proxy) {
		this.proxy = proxy;
	}


	/**
	 *  Gets the Open attribute of the Edge object
	 *
	 *@return    The Open value
	 *@since     June 6, 2002
	 */
	public abstract boolean isOpen();


	/**
	 *  Description of the Method
	 *
	 *@exception  IOException  Description of Exception
	 *@since                   April 2, 2002
	 */
	public abstract void init() throws IOException;



	// Operations
	/**
	 *  An operation that does read one DataRecord from Edge
	 *
	 *@param  record                    Description of Parameter
	 *@return                           Description of the Returned Value
	 *@exception  IOException           Description of Exception
	 *@exception  InterruptedException  Description of Exception
	 *@since                            April 2, 2002
	 */

	public abstract DataRecord readRecord(DataRecord record) throws IOException, InterruptedException;


	/**
	 *  Gets the recordCounter attribute of the Edge object
	 *
	 *@return    The recordCounter value
	 */
	public abstract int getRecordCounter();


	/**
	 *  Description of the Method
	 *
	 *@param  record                    Description of Parameter
	 *@return                           True if success, otherwise false (if no
	 *      more data)
	 *@exception  IOException           Description of Exception
	 *@exception  InterruptedException  Description of Exception
	 *@since                            August 13, 2002
	 */
	public abstract boolean readRecordDirect(ByteBuffer record) throws IOException, InterruptedException;


	/**
	 *  An operation that does send one DataRecord through the Edge/PIPE
	 *
	 *@param  record                    Description of Parameter
	 *@exception  IOException           Description of Exception
	 *@exception  InterruptedException  Description of Exception
	 *@since                            April 2, 2002
	 */
	public abstract void writeRecord(DataRecord record) throws IOException, InterruptedException;


	/**
	 *  Description of the Method
	 *
	 *@param  record                    Description of Parameter
	 *@exception  IOException           Description of Exception
	 *@exception  InterruptedException  Description of Exception
	 *@since                            August 13, 2002
	 */
	public abstract void writeRecordDirect(ByteBuffer record) throws IOException, InterruptedException;


	/**
	 *  Description of the Method
	 *
	 *@since    April 2, 2002
	 */
	public abstract void open();


	/**
	 *  Description of the Method
	 *
	 *@since    April 2, 2002
	 */
	public abstract void close();
	
	
	public abstract boolean hasData();
}
/*
 *  end class Edge
 */

