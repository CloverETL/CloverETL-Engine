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
import org.jetel.util.bytes.CloverBuffer;

/**
 *  A class that represents Edge - data connection between two NODEs
 *
 *@author      D.Pavlis
 *@created     4. srpen 2003
 *@since       April 2, 2002
 *@see         org.jetel.graph.InputPort
 *@see         org.jetel.graph.OutputPort
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
	 * @see GraphElement#preExecute()
	 */
	public void preExecute() {
		//empty implementation
    }
    
	/**
	 * @see GraphElement#postExecute()
	 */
	public void postExecute() {
		//empty implementation
	}

	/**
	 * Resets all internal settings to the initial state.
	 */
	@Deprecated
	public void reset() {
		//empty
	}
	
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
    public abstract long getOutputRecordCounter();

    /**
     *  Gets the recordCounter - how many records came out this edge so far.
     *
     *@return    The recordCounter value
     */
    public abstract long getInputRecordCounter();

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
	 * @return size of allocated memory on the edge (memory footprint)
     */
    public abstract int getUsedMemory();
    
	/**
	 * Available only in graph verbose mode.
	 * @return aggregated time how long the reader thread waits for data
	 */
    public long getReaderWaitingTime() {
    	return 0;
    }
    
	/**
	 * Available only in graph verbose mode.
	 * @return aggregated time how long the writer thread waits for data
	 */
    public long getWriterWaitingTime() {
    	return 0;
    }
    
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
	public abstract boolean readRecordDirect(CloverBuffer record) throws IOException, InterruptedException;


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
	public abstract void writeRecordDirect(CloverBuffer record) throws IOException, InterruptedException;

    public abstract void eof() throws IOException, InterruptedException;
    
    public abstract boolean isEOF();
    
    public abstract void free();
    
	public abstract boolean hasData();
}
/*
 *  end class Edge
 */

