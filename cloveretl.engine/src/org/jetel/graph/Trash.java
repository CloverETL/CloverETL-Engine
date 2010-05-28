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
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * A class that represents Trash port - any data written into it is immediately discarded
 *
 * @author      D.Pavlis
 * @since       November 1, 2002
 * @see         org.jetel.graph.OutputPort
 * @revision    $Revision$
 */
public class Trash implements OutputPort, OutputPortDirect {
    private int portNum;
    
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
	 *  Description of the Field
	 *
	 * @since    April 18, 2002
	 */
	protected int recordCounter;

    protected long byteCounter;

	// Attributes

	/**
	 *Constructor for the Edge object
	 *
	 * @param  id        Description of Parameter
	 * @param  metadata  Description of Parameter
	 * @since            April 2, 2002
	 */
	public Trash(String id, DataRecordMetadata metadata) {
		this.id = id;
		this.metadata = metadata;
		this.graph = null;
		writer = null;
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
    
    public long getByteCounter(){
        return byteCounter;
    }

    public int getOutputRecordCounter() {
        return recordCounter;
    }
    
    public long getOutputByteCounter(){
        return byteCounter;
    }

	/**
	 *  Gets the Writer attribute of the Edge object
	 *
	 * @return    The Writer value
	 * @since     May 21, 2002
	 */

	public Node getReader() {
		return null;
	}


	/**
	 *  Gets the Open attribute of the Edge object - Always open
	 *
	 * @return    The Open value
	 * @since     June 6, 2002
	 */
	public boolean isOpen() {
		return true;
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
        byteCounter=0;
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
		recordCounter++;
        byteCounter+=record.getSizeSerialized();
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
		byteCounter+=record.remaining();
        recordCounter++;
	}

	
	/**
	 * An operation that does ...
	 *
	 * @param  writer   Description of Parameter
	 * @since           April 2, 2002
	 */
	public void connectWriter(Node writer, int portNum) {
		this.writer = writer;
        this.portNum = portNum;
	}


	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public void open() {
	}


	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public void close() {
	}


    public int getOutputPortNumber() {
        return portNum;
    }


    public void eof() throws InterruptedException {
        
    }

    /*
     * (non-Javadoc)
     * @see org.jetel.graph.OutputPort#reset()
     */
	public void reset() throws ComponentNotReadyException {
		recordCounter = 0;
        byteCounter=0;
	}

}
/*
 *  end class Trash
 */

