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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataStub;
import org.jetel.metadata.MetadataFactory;
import org.jetel.util.bytes.CloverBuffer;
import org.w3c.dom.Element;

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
    protected int debugMaxRecords;
    protected boolean debugLastRecords;
    protected String debugFilterExpression;
    protected boolean debugSampleData;
    
    private boolean eofSent;
    
	private EdgeTypeEnum edgeType;

	private EdgeBase edge;

	/**
	 *  Constructor for the EdgeStub object
	 *
	 * @param  id        unique identification of the Edge
	 * @param  metadata  Metadata describing data transported by this edge
	 * @since            April 2, 2002
	 */
	public Edge(String id, DataRecordMetadata metadata, boolean debugMode) {
        super(id);
		this.metadata = metadata;
        this.debugMode = debugMode;
		reader = writer = null;
    	edgeType = EdgeTypeEnum.DIRECT;
		edge = null;
		eofSent = false;
	}

    public Edge(String id, DataRecordMetadata metadata) {
        this(id, metadata, false);
    }
    
	public Edge(String id, DataRecordMetadataStub metadataStub) {
		this(id, null, false);
		this.metadataStub=metadataStub;
	}

	public Edge(String id, DataRecordMetadataStub metadataStub, DataRecordMetadata metadata, boolean debugMode) {
		this(id,metadata, debugMode);
		this.metadataStub=metadataStub;
	}

    public void setDebugMode(boolean debugMode) {
    	this.debugMode = debugMode;
    }
    
    public void setDebugMaxRecords(int debugMaxRecords) {
    	this.debugMaxRecords = debugMaxRecords;
    }
    
    public void setDebugLastRecords(boolean debugLastRecords) {
    	this.debugLastRecords = debugLastRecords;
    }
    
    public void setFilterExpression(String filterExpression) {
    	this.debugFilterExpression = filterExpression;
    }
    
    public void setDebugSampleData(boolean debugSampleData) {
    	this.debugSampleData = debugSampleData;
    }
    
	/**
	 *  Sets the type attribute of the EdgeProxy object
	 *
	 * @param  edgeType  The new type value
	 */
	public void setEdgeType(EdgeTypeEnum edgeType) {
		this.edgeType = edgeType;
	}

	/**
	 *  Gets the type attribute of the Edge object
	 *
	 * @return    The type value
	 */
	public EdgeTypeEnum getEdgeType() {
		return edgeType;
	}

	/**
	 *  Gets the Metadata attribute of the Edge object
	 *
	 * @return    The Metadata value
	 * @since     April 4, 2002
	 */
	@Override
	public DataRecordMetadata getMetadata() {
		return metadata;
	}


	/* (non-Javadoc)
	 * @see org.jetel.graph.InputPort#getRecordCounter()
	 */
	@Override
	public int getRecordCounter() {
		return edge.getOutputRecordCounter();
	}
    
    /* (non-Javadoc)
     * @see org.jetel.graph.OutputPort#getOutputRecordCounter()
     */
    @Override
	public int getOutputRecordCounter() {
        return edge.getOutputRecordCounter();
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.InputPort#getInputRecordCounter()
     */
    @Override
	public int getInputRecordCounter() {
        return edge.getInputRecordCounter();
    }

	/* (non-Javadoc)
	 * @see org.jetel.graph.InputPort#getByteCounter()
	 */
	@Override
	public long getByteCounter(){
	    return edge.getOutputByteCounter();
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.OutputPort#getOutputByteCounter()
     */
    @Override
	public long getOutputByteCounter(){
        return edge.getOutputByteCounter();
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.InputPort#getInputByteCounter()
     */
    @Override
	public long getInputByteCounter(){
        return edge.getInputByteCounter();
    }

    public int getBufferedRecords(){
    	return edge.getBufferedRecords();
    }

	@Override
	public int getUsedMemory() {
		return edge.getUsedMemory();
	}

	/**
	 *  Gets the Reader attribute of the Edge object
	 *
	 * @return    The Reader value
	 * @since     May 21, 2002
	 */
	@Override
	public Node getReader() {
		return reader;
	}


	/**
	 *  Gets the Writer attribute of the Edge object
	 *
	 * @return    The Writer value
	 * @since     May 21, 2002
	 */
	@Override
	public Node getWriter() {
		return writer;
	}


	/**
	 *  Gets the Open attribute of the Edge object
	 *
	 * @return    The Open value
	 * @since     June 6, 2002
     * @deprecated use hasData() method instead
	 */
	@Override
	public boolean isOpen() {
		return !isEOF();
	}

    @Override
	public boolean isEOF() {
        return edge.isEOF();
    }

	/**
	 *  This method creates appropriate version of Edge (direct or buffered)
	 *  based on specified type and then initializes it.
	 *
	 * @exception  IOException  Description of Exception
	 * @since                   April 2, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
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
				throw new ComponentNotReadyException("Creating metadata from db connection failed: ", ex);
			}
		}
		if (edge == null) {
			edge = edgeType.createEdgeBase(this);
		}
        try {
            edge.init();
        } catch (Exception ex){
            throw new ComponentNotReadyException(this, ex);
        }
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#preExecute()
	 */
	@Override
	public synchronized void preExecute() throws ComponentNotReadyException {
		super.preExecute();

		if (debugMode && getGraph().isDebugMode()) {
            String debugFileName = getDebugFileName();
            logger.debug("Edge '" + getId() + "' is running in debug mode. (" + debugFileName + ")");
            edgeDebuger = new EdgeDebuger(this, debugFileName, false, debugMaxRecords, debugLastRecords,
            				debugFilterExpression, metadata, debugSampleData);
            try {
                edgeDebuger.init();
            } catch (Exception ex){
                throw new ComponentNotReadyException(this, ex);
            }
        }
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		
		eofSent = false;
		edge.reset();
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#postExecute(org.jetel.graph.TransactionMethod)
	 */
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		
        if (edgeDebuger != null) {
            edgeDebuger.close();
            edgeDebuger = null;
        }
	}
	
    /**
     * NOTE: same implementation must be also in clover gui Connector.getDebugFileName()
     * @return absolute path to debug file
     */
    private String getDebugFileName() {
        GraphRuntimeContext runtimeContext = getGraph().getRuntimeContext();
		String tmpFile = runtimeContext.getDebugDirectory();
        
        if(!tmpFile.endsWith(System.getProperty("file.separator"))) {
            tmpFile += System.getProperty("file.separator");
        }
        tmpFile += runtimeContext.getRunId() + "-" + getId() + ".dbg";

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

	@Override
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
	@Override
	public boolean readRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		return edge.readRecordDirect(record);
	}

	/**
	 * @deprecated use {@link #readRecordDirect(CloverBuffer)}
	 */
	@Override
	@Deprecated
	public boolean readRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
		CloverBuffer wrappedBuffer = CloverBuffer.wrap(record);
		boolean result = readRecordDirect(wrappedBuffer);
		if (wrappedBuffer.buf() != record) {
			throw new JetelRuntimeException("Deprecated method invokation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
		return result;
	}

	/**
	 *  An operation that does send one DataRecord through the Edge/PIPE
	 *
	 * @param  record                    Description of Parameter
	 * @exception  IOException           Description of Exception
	 * @exception  InterruptedException  Description of Exception
	 * @since                            April 2, 2002
	 */
	@Override
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
	@Override
	public void writeRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
        if(edgeDebuger != null) {
            edgeDebuger.writeRecord(record);
            record.rewind();
        }
		edge.writeRecordDirect(record);
	}

	/**
	 * @deprecated use {@link #writeRecordDirect(CloverBuffer)}
	 */
	@Override
	@Deprecated
	public void writeRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
		writeRecordDirect(CloverBuffer.wrap(record));
	}

	/**
	 *  An operation that does ...
	 *
	 * @param  _reader  Description of Parameter
	 * @since           April 2, 2002
	 */
	@Override
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
	@Override
	public void connectWriter(Node _writer, int portNum) {
		this.writer = _writer;
        this.writerPort = portNum;
	}


	/**
	 *  Description of the Method
	 *
	 * @deprecated 
	 */
	@Override
	public void open() {
        //DO NOTHING
	}


	/**
     * @throws IOException 
	 * @deprecated use direct eof() method
	 */
	@Override
	public void close() throws InterruptedException, IOException {
        eof();
	}
	
    /* (non-Javadoc)
     * @see org.jetel.graph.OutputPort#eof()
     */
    @Override
	public void eof() throws InterruptedException, IOException {
    	if (!eofSent) {
        	edge.eof();
        	eofSent = true;
    	}
    }
    
	@Override
	public boolean hasData(){
		return edge.hasData();
	}

    @Override
	public int getInputPortNumber() {
        return readerPort;
    }

    @Override
	public int getOutputPortNumber() {
        return writerPort;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#checkConfig()
     */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        //TODO
        return status;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#free()
     */
    @Override
	public void free() {
        if(!isInitialized()) return;
        super.free();

        edge.free();
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

	public void toXML(Element xmlElement) {
		// TODO Auto-generated method stub
		
	}
}
/*
 *  end class EdgeStub
 */

