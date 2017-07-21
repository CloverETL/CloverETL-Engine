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
import org.jetel.component.RemoteEdgeComponent;
import org.jetel.data.DataRecord;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataStub;
import org.jetel.metadata.MetadataFactory;
import org.jetel.util.EdgeDebugUtils;
import org.jetel.util.bytes.CloverBuffer;

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
 * @see EdgeFactory
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
    protected long debugMaxRecords;
    protected boolean debugLastRecords;
    protected String debugFilterExpression;
    protected boolean debugSampleData;
    
    private boolean eofSent;
    
	protected EdgeTypeEnum edgeType;

	protected EdgeBase edge;

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
	
	/**
	 * Copies settings from the given edge to this edge.
	 * The otherEdge has to be from same graph as this edge. For example referenced metadata are not copied
	 * from otherGraph to thisGraph. 
	 * @param otherEdge
	 */
	public void copySettingsFrom(Edge otherEdge) {
		this.metadata = otherEdge.metadata;
		this.metadataStub = otherEdge.metadataStub;
		this.debugMode = otherEdge.debugMode;
		this.debugMaxRecords = otherEdge.debugMaxRecords;
		this.debugLastRecords = otherEdge.debugLastRecords;
		this.debugFilterExpression = otherEdge.debugFilterExpression;
		this.debugSampleData = otherEdge.debugSampleData;
		this.edgeType = otherEdge.edgeType;
	}

    public void setDebugMode(boolean debugMode) {
    	this.debugMode = debugMode;
    }
    
    public void setDebugMaxRecords(long debugMaxRecords) {
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
		return edgeType != null ? edgeType : EdgeTypeEnum.DIRECT;
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
	public long getRecordCounter() {
		return edge.getOutputRecordCounter();
	}
    
    /* (non-Javadoc)
     * @see org.jetel.graph.OutputPort#getOutputRecordCounter()
     */
    @Override
	public long getOutputRecordCounter() {
        return edge.getOutputRecordCounter();
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.InputPort#getInputRecordCounter()
     */
    @Override
	public long getInputRecordCounter() {
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
	 * Available only in graph verbose mode.
	 * @return aggregated time how long the reader thread waits for data
	 */
	@Override
	public long getReaderWaitingTime() {
		return edge.getReaderWaitingTime();
	}
	
	/**
	 * Available only in graph verbose mode.
	 * @return aggregated time how long the writer thread waits for data
	 */
	@Override
	public long getWriterWaitingTime() {
		return edge.getWriterWaitingTime();
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
	 * @return true if this edge is remote; either reader of this edge
	 * is RemoteEdgeDataTransmitter or writer of this edge is RemoteEdgeDataReceiver;
	 * {@link #getReaderRunId()} differs from {@link #getWriterRunId()}
	 */
	public boolean isRemote() {
		return getReaderRunId() != getWriterRunId();
	}
	
	/**
	 * @return runId of reader component;
	 * {@link RemoteEdgeComponent} is considered as component
	 * which is running on different worker
	 */
	public long getReaderRunId() {
		if (reader instanceof RemoteEdgeComponent) {
			return ((RemoteEdgeComponent) reader).getRemoteRunId();
		} else {
			return reader.getGraph().getRuntimeContext().getRunId();
		}
	}

	/**
	 * @return runId of writer component
	 * {@link RemoteEdgeComponent} is considered as component
	 * which is running on different worker
	 */
	public long getWriterRunId() {
		if (writer instanceof RemoteEdgeComponent) {
			return ((RemoteEdgeComponent) writer).getRemoteRunId();
		} else {
			return writer.getGraph().getRuntimeContext().getRunId();
		}
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
			edge = getEdgeType().createEdgeBase(this);
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

		edge.preExecute();
		
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
		
		edge.postExecute();
		
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
        tmpFile += EdgeDebugUtils.getDebugFileName(getWriterRunId(), getReaderRunId(), getId());

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
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
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
    	if (edgeDebuger != null) {
    		edgeDebuger.eof();
    	}
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
        
        //check phase order
        if (reader.getPhaseNum() < writer.getPhaseNum()) {
        	status.add(new ConfigurationProblem("Invalid phase order", Severity.ERROR, reader, Priority.NORMAL));
        	status.add(new ConfigurationProblem("Invalid phase order", Severity.ERROR, writer, Priority.NORMAL));
        }
        
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

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    public int hashCodeIdentity() {
    	return super.hashCode();
    }
    
    @Override
    public boolean equals(Object obj){
        if (obj instanceof Edge){
            return ((Edge)obj).getId().equals(getId());
        }else{
            return false;
        }
    }

	/**
	 * Sets specific {@link EdgeBase} instance which is used as real edge algorithm.
	 * By default, the {@link EdgeBase} instance is created in initialisation time
	 * based on {@link EdgeTypeEnum}.
	 * @param edge
	 */
	public void setEdge(EdgeBase edge) {
		this.edge = edge;
	}

	/**
	 * @see InputPort#getEdge()
	 * @see OutputPort#getEdge()
	 */
	@Override
	public Edge getEdge() {
		return this;
	}

	/**
	 * @return internal edge implementation
	 */
	public EdgeBase getEdgeBase() {
		return edge;
	}
	
	@Override
	public String toString() {
		return getId();
	}
	
}
/*
 *  end class EdgeStub
 */

