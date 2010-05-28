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

	protected Edge proxy;


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
	 *  Description of the Method
	 *
	 *@exception  IOException  Description of Exception
	 *@since                   April 2, 2002
	 */
	public abstract void init() throws IOException, InterruptedException;


	/**
	 * Resets all internal settings to the initial state.
	 */
	public abstract void reset();
	
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
     *  Gets the recordCounter - how many records came in this edge so far.
     *
     *@return    The recordCounter value
     */
    public abstract int getOutputRecordCounter();

    /**
     *  Gets the recordCounter - how many records came out this edge so far.
     *
     *@return    The recordCounter value
     */
    public abstract int getInputRecordCounter();

    /**
     * Gets the byteCounter - how many bytes came in this edge so far.
     * 
     * @return The byteCounter value
     */
    public abstract long getOutputByteCounter();

    /**
     * Gets the byteCounter - how many bytes came out this edge so far.
     * 
     * @return The byteCounter value
     */
    public abstract long getInputByteCounter();

    
    /**
     * Gets number of records currently buffered within this
     * edge
     * 
     * @return
     * @since 28.11.2006
     */
    public abstract int getBufferedRecords();
    
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

    public abstract void eof() throws IOException, InterruptedException;
    
    public abstract boolean isEOF();
    
    public abstract void free();
    
	public abstract boolean hasData();
}
/*
 *  end class Edge
 */

