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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataStub;
import org.jetel.metadata.MetadataFactory;

/**
 *  A class that represents Edge Proxy - surrogate which directs calls to
 *  apropriate Edge implementation according to edge type specification
 *
 * @author      D.Pavlis
 * @since       August 3, 2003
 * @see         org.jetel.graph.DirectEdge
 * @see         org.jetel.graph.PhaseConnectionEdge
 * @see        org.jetel.graph.InputPort
 * @see        org.jetel.graph.OutputPort
 * @revision   $Revision$
 */
public class Edge extends GraphElement implements InputPort, OutputPort, InputPortDirect, OutputPortDirect {

    private static Log logger = LogFactory.getLog(Edge.class);

	protected Node reader;
	protected Node writer;
    
    protected int readerPort;
    protected int writerPort;

	protected DataRecordMetadata metadata;
	protected DataRecordMetadataStub metadataStub;

    protected boolean debugMode;
    protected EdgeDebuger edgeDebuger;
    
	private int edgeType;

    /**
     * Distinct DirectEdge and DirectEdgeFastPropagate inner implementation of the edge.
     * Used only for EDGE_TYPE_DIRECT.
     */
    private boolean fastPropagate = false;
    
	private EdgeBase edge;

	/**  Proxy represents Direct Edge */
	public final static int EDGE_TYPE_DIRECT = 0;
	/**  Proxy represents Buffered Edge */
	public final static int EDGE_TYPE_BUFFERED = 1;
	/** Proxy represents Edge connecting two different phases */
	public final static int EDGE_TYPE_PHASE_CONNECTION = 2;


	/**
	 *  Constructor for the EdgeStub object
	 *
	 * @param  id        unique identification of the Edge
	 * @param  metadata  Metadata describing data transported by this edge
	 * @since            April 2, 2002
	 */
	public Edge(String id, DataRecordMetadata metadata, boolean debugMode, boolean fastPropagate) {
        super(id);
		this.metadata = metadata;
        this.debugMode = debugMode;
		reader = writer = null;
		edgeType = EDGE_TYPE_DIRECT;//default edge is direct (no outside buffering necessary)
		edge = null;
        this.fastPropagate = fastPropagate;
	}

    public Edge(String id, DataRecordMetadata metadata) {
        this(id, metadata, false, false);
    }
    
	public Edge(String id, DataRecordMetadataStub metadataStub,DataRecordMetadata metadata, boolean debugMode, boolean fastPropagate) {
		this(id,metadata, debugMode, fastPropagate);
		this.metadataStub=metadataStub;
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
	public void init() throws ComponentNotReadyException {
		/* if metadata is null and we have metadata stub, try to
		 * load metadata from JDBC
		 */
		if (metadata==null){
			if (metadataStub==null){
				throw new RuntimeException("No metadata and no metadata stub defined for edge: "+getId());
			}
			try{
				metadata=MetadataFactory.fromStub(metadataStub);
			}catch(Exception ex){
				throw new ComponentNotReadyException(ex.getMessage());
			}
		}
		if (edge == null) {
			if (edgeType == EDGE_TYPE_BUFFERED) {
			    edge = new BufferedEdge(this);
			} else if (edgeType == EDGE_TYPE_PHASE_CONNECTION ){
			    edge = new PhaseConnectionEdge(this);
			} else {
				edge = fastPropagate ? (EdgeBase) new DirectEdgeFastPropagate(this) : new DirectEdge(this);
			}
		}
        if(debugMode) {
            String debugFileName = getDebugFileName();
            logger.debug("Edge '" + getId() + "' is running in debug mode. (" + debugFileName + ")");
            edgeDebuger = new EdgeDebuger(debugFileName, false);
            try{
                edgeDebuger.init();
            }catch(IOException ex){
                throw new ComponentNotReadyException(ex.getMessage());
            }
        }
        try{
            edge.init();
        }catch(IOException ex){
            throw new ComponentNotReadyException(ex.getMessage());
        }
	}

    /**
     * NOTE: same implementation must be also in clover gui Connector.getDebugFileName()
     * @return absoute path to debug file
     */
    private String getDebugFileName() {
        String tmpFile = getGraph().getDebugDirectory();
        
        if(!tmpFile.endsWith(System.getProperty("file.separator"))) {
            tmpFile += System.getProperty("file.separator");
        }
        tmpFile += getId() + ".dbg";

        return tmpFile;
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
        if(edgeDebuger != null) edgeDebuger.writeRecord(record);
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
        if(edgeDebuger != null) {
            edgeDebuger.writeRecord(record);
            record.rewind();
        }
		edge.writeRecordDirect(record);
	}


	/**
	 *  An operation that does ...
	 *
	 * @param  _reader  Description of Parameter
	 * @since           April 2, 2002
	 */
	public void connectReader(Node _reader, int portNum) {
		this.reader = _reader;
        this.readerPort = portNum;
	}


	/**
	 *  An operation that does ...
	 *
	 * @param  _writer  Description of Parameter
	 * @since           April 2, 2002
	 */
	public void connectWriter(Node _writer, int portNum) {
		this.writer = _writer;
        this.writerPort = portNum;
	}


	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public void open() {
		edge.open();
        if(edgeDebuger != null) edgeDebuger.open();
	}


	/**
	 *  Description of the Method
	 *
	 * @since    April 2, 2002
	 */
	public void close() {
		edge.close();
        if(edgeDebuger != null) edgeDebuger.close();
	}
	
	public boolean hasData(){
		return edge.hasData();
	}

    public int getInputPortNumber() {
        return readerPort;
    }

    public int getOutputPortNumber() {
        return writerPort;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#checkConfig()
     */
    public boolean checkConfig() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#free()
     */
    public void free() {
        close();
    }

    public boolean isFastPropagate() {
        return fastPropagate;
    }

    public void setFastPropagate(boolean fastPropagate) {
        this.fastPropagate = fastPropagate;
    }
    
    @Override public int hashCode(){
        return getId().hashCode();
    }

    @Override public boolean equals(Object obj){
        if (obj instanceof Edge){
            return ((Edge)obj).getId().equals(getId());
        }else{
            return false;
        }
    }
}
/*
 *  end class EdgeStub
 */

