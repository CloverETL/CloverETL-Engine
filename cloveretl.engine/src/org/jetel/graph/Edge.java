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
package org.jetel.graph;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.graph.InputPort;
import org.jetel.graph.OutputPort;
import org.jetel.util.StringUtils;
import org.jetel.exception.InvalidGraphObjectNameException;

/**
 *  A class that represents Edge Proxy - surrogate which directs calls to
 *  apropriate Edge implementation according to edge type specification
 *
 * @author      D.Pavlis
 * @since       August 3, 2003
 * @see         org.jetel.graph.DirectEdge
 * @see         org.jetel.graph.BufferedEdge
 * @see        org.jetel.graph.InputPort
 * @see        org.jetel.graph.OutputPort
 * @revision   $Revision$
 */
public class Edge implements InputPort, OutputPort, InputPortDirect, OutputPortDirect {

	protected TransformationGraph graph;
	protected String id;

	protected Node reader;
	protected Node writer;

	protected DataRecordMetadata metadata;

	private int edgeType;

	private EdgeBase edge;

	/**  Proxy represents Direct Edge */
	public final static int EDGE_TYPE_DIRECT = 0;
	/**  Proxy represents Buffered Edge */
	public final static int EDGE_TYPE_BUFFERED = 1;


	/**
	 *  Constructor for the EdgeStub object
	 *
	 * @param  id        unique identification of the Edge
	 * @param  metadata  Metadata describing data transported by this edge
	 * @since            April 2, 2002
	 */
	public Edge(String id, DataRecordMetadata metadata) {
		if (!StringUtils.isValidObjectName(id)){
			throw new InvalidGraphObjectNameException(id,"EDGE");
		}
		this.id = id;
		this.metadata = metadata;
		this.graph = null;
		reader = writer = null;
		edgeType = EDGE_TYPE_DIRECT;//default edge is direct (no outside buffering necessary)
		edge = null;
	}


	/**
	 *  Sets the type attribute of the EdgeProxy object
	 *
	 * @param  edgeType  The new type value
	 */
	public void setType(int edgeType) {
		this.edgeType = edgeType;
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
	 *  An operation that does ...
	 *
	 * @return    The ID value
	 * @since     April 2, 2002
	 */
	public String getID() {
		return id;
	}


	/**
	 *  Gets the type attribute of the Edge object
	 *
	 * @return    The type value
	 */
	public int getType() {
		return edgeType;
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
		return edge.getRecordCounter();
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
		return edge.isOpen();
	}


	/**
	 *  This method creates appropriate version of Edge (direct or buffered)
	 *  based on specified type and then initializes it.
	 *
	 * @exception  IOException  Description of Exception
	 * @since                   April 2, 2002
	 */
	public void init() throws IOException {
		if (edge == null) {
			if (edgeType == EDGE_TYPE_BUFFERED) {
				edge = new BufferedEdge(this);
			} else {
				edge = new DirectEdge(this);
			}
		}
		edge.init();
	}



	// Operations
	/**
	 *  An operation that does read one DataRecord from Edge
	 *
	 * @param  record                    Description of Parameter
	 * @return                           Description of the Returned Value
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            April 2, 2002
	 */

	public DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
		return edge.readRecord(record);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  record                    Description of Parameter
	 * @return                           True if success, otherwise false (if no
	 *      more data)
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            August 13, 2002
	 */
	public boolean readRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
		return edge.readRecordDirect(record);
	}


	/**
	 *  An operation that does send one DataRecord through the Edge/PIPE
	 *
	 * @param  record                    Description of Parameter
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            April 2, 2002
	 */
	public void writeRecord(DataRecord record) throws IOException, InterruptedException {
		edge.writeRecord(record);
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
		edge.writeRecordDirect(record);
	}


	/**
	 *  An operation that does ...
	 *
	 * @param  _reader  Description of Parameter
	 * @since           April 2, 2002
	 */
	public void connectReader(Node _reader) {
		this.reader = _reader;
	}


	/**
	 *  An operation that does ...
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
		edge.open();
	}


	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public void close() {
		edge.close();
	}
	
	public boolean hasData(){
		return edge.hasData();
	}
}
/*
 *  end class EdgeStub
 */

