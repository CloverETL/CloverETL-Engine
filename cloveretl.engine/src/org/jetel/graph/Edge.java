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
import org.jetel.data.Defaults;
import org.jetel.enums.EdgeDebugMode;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataStub;
import org.jetel.metadata.MetadataFactory;
import org.jetel.util.CloverPublicAPI;
import org.jetel.util.EdgeDebugUtils;
import org.jetel.util.ReferenceState;
import org.jetel.util.ReferenceUtils;
import org.jetel.util.bytes.CloverBuffer;

/**
 *  A class that represents Edge Proxy - surrogate which directs calls to
 *  appropriate Edge implementation according to edge type specification
 *
 * @author      D.Pavlis
 * @since       August 3, 2003
 * @see         org.jetel.graph.DirectEdge
 * @see         org.jetel.graph.PhaseConnectionEdge
 * @see        org.jetel.graph.InputPort
 * @see        org.jetel.graph.OutputPort
 * @see EdgeFactory
 */
@CloverPublicAPI
public class Edge extends GraphElement implements InputPort, OutputPort, InputPortDirect, OutputPortDirect {

    private static Log logger = LogFactory.getLog(Edge.class);

	protected Node reader;
	protected Node writer;
    
    protected int readerPort;
    protected int writerPort;

	protected DataRecordMetadata metadata;
	/** Reference to a graph element, from where the metadata should be derived. */
	protected String metadataRef;
	/** State of the reference to a graph element*/
	protected ReferenceState metadataRefState;
	
    protected EdgeDebugMode debugMode;
    protected EdgeDebugWriter edgeDebugWriter;
    protected long debugMaxRecords;
    protected boolean debugLastRecords;
    protected String debugFilterExpression;
    protected boolean debugSampleData;
    
    private boolean eofSent;
    
	protected EdgeTypeEnum edgeType;

	protected EdgeBase edge;

	/**
	 * True if the edge base ({@link #edge}) is not under complete control
	 * of this edge. The edge base is shared with other edge (from parent graph).
	 * Some operations like {@link #preExecute()}, {@link #postExecute()} and {@link #free()}
	 * are performed only by the real owner of the edge base.
	 * This functionality is used for edges between parent graph and subgraph.
	 * These edge couples can share edge base, which allows direct data passing
	 * from parent graph to subgraph and backward without special copy threads.
	 * This is true if sharing is caused by writer component.
	 */
	private boolean sharedEdgeBaseFromWriter = false;
	
	/**
	 * True if the edge base ({@link #edge}) is not under complete control
	 * of this edge. The edge base is shared with other edge (from parent graph).
	 * Some operations like {@link #preExecute()}, {@link #postExecute()} and {@link #free()}
	 * are performed only by the real owner of the edge base.
	 * This functionality is used for edges between parent graph and subgraph.
	 * These edge couples can share edge base, which allows direct data passing
	 * from parent graph to subgraph and backward without special copy threads.
	 * This is true if sharing is caused by reader component.
	 */
	private boolean sharedEdgeBaseFromReader = false;
	
	/**
	 *  Constructor for the EdgeStub object
	 *
	 * @param  id        unique identification of the Edge
	 * @param  metadata  Metadata describing data transported by this edge
	 * @since            April 2, 2002
	 */
	public Edge(String id, DataRecordMetadata metadata, EdgeDebugMode debugMode) {
        super(id);
		this.metadata = metadata;
        this.debugMode = debugMode != null ? debugMode : EdgeDebugMode.DEFAULT;
		reader = writer = null;
		edge = null;
		eofSent = false;
	}

    public Edge(String id, DataRecordMetadata metadata) {
        this(id, metadata, null);
    }
    
	public Edge(String id, DataRecordMetadataStub metadataStub) {
		this(id, createMetadataFromStub(metadataStub), null);
	}

	private static DataRecordMetadata createMetadataFromStub(DataRecordMetadataStub metadataStub) {
		if (metadataStub != null) {
			return MetadataFactory.fromStub(metadataStub);
		} else {
			return null;
		}
	}

	/**
	 * Copies settings from the given edge to this edge.
	 * The otherEdge has to be from same graph as this edge. For example referenced metadata are not copied
	 * from otherGraph to thisGraph. 
	 * @param otherEdge
	 */
	void copySettingsFrom(Edge otherEdge) {
		this.metadata = otherEdge.metadata;
		this.debugMode = otherEdge.debugMode;
		this.debugMaxRecords = otherEdge.debugMaxRecords;
		this.debugLastRecords = otherEdge.debugLastRecords;
		this.debugFilterExpression = otherEdge.debugFilterExpression;
		this.debugSampleData = otherEdge.debugSampleData;
		this.edgeType = otherEdge.edgeType;
	}

    public void setDebugMode(EdgeDebugMode debugMode) {
    	this.debugMode = debugMode != null ? debugMode : EdgeDebugMode.DEFAULT;
    }
    
    public EdgeDebugMode getDebugMode() {
    	return debugMode;
    }
    
    public boolean isEdgeDebugging() {
    	return getGraph().isEdgeDebugging()
    			&& debugMode != EdgeDebugMode.OFF;
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

	public void setMetadata(DataRecordMetadata metadata) {
		this.metadata = metadata;
	}
	
	/**
	 * @return reference to a graph element, from where metadata for this edge should be derived
	 */
	public String getMetadataRef() {
		return metadataRef;
	}
	
	public void setMetadataRef(String metadataRef) {
		this.metadataRef = metadataRef;
	}
	
	public ReferenceState getMetadataReferenceState() {
		return metadataRefState;
	}
	
	public void setMetadataReferenceState(ReferenceState refState) {
		this.metadataRefState = refState;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.InputPort#getRecordCounter()
	 */
	@Override
	public long getRecordCounter() {
		return getOutputRecordCounter();
	}
    
    /* (non-Javadoc)
     * @see org.jetel.graph.OutputPort#getOutputRecordCounter()
     */
    @Override
	public long getOutputRecordCounter() {
    	return edge != null ? edge.getOutputRecordCounter() : 0;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.InputPort#getInputRecordCounter()
     */
    @Override
	public long getInputRecordCounter() {
    	return edge != null ? edge.getInputRecordCounter() : 0;
    }

	/* (non-Javadoc)
	 * @see org.jetel.graph.InputPort#getByteCounter()
	 */
	@Override
	public long getByteCounter( ) {
	    return getOutputByteCounter();
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.OutputPort#getOutputByteCounter()
     */
    @Override
	public long getOutputByteCounter() {
    	return edge != null ? edge.getOutputByteCounter() : 0;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.InputPort#getInputByteCounter()
     */
    @Override
	public long getInputByteCounter() {
    	return edge != null ? edge.getInputByteCounter() : 0;
    }

    public int getBufferedRecords() {
    	return edge != null ? edge.getBufferedRecords() : 0;
    }

	@Override
	public int getUsedMemory() {
		return edge != null ? edge.getUsedMemory() : 0;
	}

	/**
	 * Available only in graph verbose mode.
	 * @return aggregated time how long the reader thread waits for data
	 */
	@Override
	public long getReaderWaitingTime() {
		return edge != null ? edge.getReaderWaitingTime() : 0;
	}
	
	/**
	 * Available only in graph verbose mode.
	 * @return aggregated time how long the writer thread waits for data
	 */
	@Override
	public long getWriterWaitingTime() {
		return edge != null ? edge.getWriterWaitingTime() : 0;
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
	@Deprecated
	@Override
	public boolean isOpen() {
		return !isEOF();
	}

    @Override
	public boolean isEOF() {
        return getEdgeBaseChecked().isEOF();
    }

    /**
     * @return true if eof() method has been already invoked on this edge
     */
    public boolean isEofSent() {
    	return getEdgeBaseChecked().isEofSent();
    }
    
    /**
     * Current thread is block until EOF on the edge is reached.
     */
    public void waitForEOF() throws InterruptedException {
    	getEdgeBaseChecked().waitForEOF();
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
		
		if (metadata == null) {
			throw new RuntimeException(createMissingMetadataMessage());
		}
		
		if (edge != null) {
			try {
				edge.init();
	        } catch (Exception ex){
	            throw new JetelRuntimeException("Edge base initialization failed.", ex);
	        }
		}
	}
	
	private void initEdgeBase() {
		if (edge == null) {
			edge = getEdgeType().createEdgeBase(this);
			try {
				edge.init();
	        } catch (Exception ex){
	            throw new JetelRuntimeException("Edge base initialization failed.", ex);
	        }
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#preExecute()
	 */
	@Override
	public synchronized void preExecute() throws ComponentNotReadyException {
		super.preExecute();

		//init edge base
		if (!isSharedEdgeBase()) {
			initEdgeBase();
		}

		eofSent = false;

		if (!isSharedEdgeBase()) {
			//pre-execute edge base only for non-shared edges
			getEdgeBaseChecked().preExecute();
		}
		
		initDebugMode();
	}

	protected void initDebugMode() {
		if (isEdgeDebugging()) {
            String debugFileName = getDebugFileName();
			switch(debugMode) {
			case ALL:
	            logger.debug("Edge '" + getId() + "' is running in debug mode ALL (" + debugFileName + ")");
	            edgeDebugWriter = new EdgeDebugWriter(this, debugFileName, metadata);
	            break;
			case DEFAULT:
	            logger.debug("Edge '" + getId() + "' is running in debug mode DEFAULT (" + debugFileName + ")");
	            edgeDebugWriter = new EdgeDebugWriter(this, debugFileName, metadata);
	            edgeDebugWriter.setDebugMaxRecords(Defaults.Graph.DEFAULT_EDGE_DEBUGGING_MAX_RECORDS);
	            edgeDebugWriter.setDebugMaxBytes(Defaults.Graph.DEFAULT_EDGE_DEBUGGING_MAX_BYTES);
	            break;
			case OFF:
	            logger.debug("Edge '" + getId() + "' has debug turned off");
				//no debugging
				break;
			case CUSTOM:
	            logger.debug("Edge '" + getId() + "' is running in debug mode CUSTOM (" + debugFileName + ")");
	            edgeDebugWriter = new EdgeDebugWriter(this, debugFileName, metadata);
	            edgeDebugWriter.setDebugMaxRecords(debugMaxRecords);
	            edgeDebugWriter.setDebugLastRecords(debugLastRecords);
	            edgeDebugWriter.setFilterExpression(debugFilterExpression);
	            edgeDebugWriter.setSampleData(debugSampleData);
				break;
			}

			try {
                edgeDebugWriter.init();
            } catch (Exception ex) {
                throw new JetelRuntimeException("Edge debugger initialisation failed.", ex);
            }
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#postExecute(org.jetel.graph.TransactionMethod)
	 */
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		
		if (!isSharedEdgeBase()) {
			//post-execute edge base only for non-shared edges
			getEdgeBaseChecked().postExecute();
		}
		
        if (edgeDebugWriter != null) {
            edgeDebugWriter.close();
            edgeDebugWriter = null;
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
		return getEdgeBaseChecked().readRecord(record);
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
		return getEdgeBaseChecked().readRecordDirect(record);
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
        if (edgeDebugWriter != null) {
        	edgeDebugWriter.writeRecord(record);
        }
		getEdgeBaseChecked().writeRecord(record);
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
        if (edgeDebugWriter != null) {
            edgeDebugWriter.writeRecord(record);
            record.rewind();
        }
        getEdgeBaseChecked().writeRecordDirect(record);
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
	@Deprecated
	@Override
	public void open() {
        //DO NOTHING
	}


	/**
     * @throws IOException 
	 * @deprecated use direct eof() method
	 */
	@Deprecated
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
        	if (edgeDebugWriter != null) {
        		edgeDebugWriter.eof();
        	}

        	getEdgeBaseChecked().eof();

        	eofSent = true;
    	}
    }
    
	@Override
	public boolean hasData() {
		return getEdgeBaseChecked().hasData();
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
        if (Phase.comparePhaseNumber(reader.getPhaseNum(), writer.getPhaseNum()) < 0) {
        	status.addError(reader, null, "Invalid phase order. Phase number of component " + reader + " is less than " + writer + "'s phase number.");
        	status.addError(writer, null, "Invalid phase order. Phase number of component " + writer + " is greater than " + reader + "'s phase number.");
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
        
        if (!isSharedEdgeBase()) {
			if (edge != null) {
				// free edge base only for non-shared edges
				edge.free();
			}
        }
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
        } else {
            return false;
        }
    }

	/**
	 * Sets specific {@link EdgeBase} instance which is used as real edge algorithm.
	 * By default, the {@link EdgeBase} instance is created in initialization time
	 * based on {@link EdgeTypeEnum}. Edge type is automatically updated as well.
	 * @param edge
	 */
	public void setEdge(EdgeBase edge) {
		this.edge = edge;
		this.edgeType = EdgeTypeEnum.valueOf(edge);
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
	 * @return true if this edge shares edge base with parent graph - see Subgraph component
	 */
	public EdgeBase getEdgeBase() {
		return edge;
	}

	/**
	 * The edge has got {@link EdgeBase} via {@link #setEdge(EdgeBase)} and
	 * this edge is not owner of the given edge base, so some operations above EdgeBase
	 * are not performed in this edge. This sharing is caused by writer component
	 * @param sharedEdgeBase
	 */
	public void setSharedEdgeBaseFromWriter(boolean sharedEdgeBaseFromWriter) {
		this.sharedEdgeBaseFromWriter = sharedEdgeBaseFromWriter;
	}

	/**
	 * The edge has got {@link EdgeBase} via {@link #setEdge(EdgeBase)} and
	 * this edge is not owner of the given edge base, so some operations above EdgeBase
	 * are not performed in this edge. This sharing is caused by reader component.
	 * @param sharedEdgeBase
	 */
	public void setSharedEdgeBaseFromReader(boolean sharedEdgeBaseFromReader) {
		this.sharedEdgeBaseFromReader = sharedEdgeBaseFromReader;
	}

	/**
	 * Input and output edges of SubgraphInput/Output components can share EdgeBase with parent graph.
	 * This is important to known, that the EdgeBase is shared. So EdgeBase
	 * initialisation, pre-execution, post-execution and freeing is not performed
	 * in this edge.
	 */
	public boolean isSharedEdgeBase() {
		return sharedEdgeBaseFromWriter || sharedEdgeBaseFromReader;
	}

	/**
	 * @return true if this edge is shared and this sharing is caused by writer component
	 */
	public boolean isSharedEdgeBaseFromWriter() {
		return sharedEdgeBaseFromWriter;
	}

	/**
	 * @return true if this edge is shared and this sharing is caused by reader component
	 */
	public boolean isSharedEdgeBaseFromReader() {
		return sharedEdgeBaseFromReader;
	}

	@Override
	public String toString() {
		return getId();
	}
	
	protected EdgeBase getEdgeBaseChecked() {
		if (edge != null) {
			return edge;
		}
		throw new JetelRuntimeException("Edge " + getId() + " has not been initilized");
	}

	private String createMissingMetadataMessage() {
		
		String defaultComponentLabel = "Unknown component";
		StringBuilder message = new StringBuilder();
		message.append("No metadata defined for edge ");
		message.append(getId());
		message.append(" (");
		message.append(getWriter() == null ? defaultComponentLabel : getWriter().getId());
		message.append(" -> ");
		message.append(getReader() == null ? defaultComponentLabel : getReader().getId());
		message.append(")");
		if (metadataRef != null) {
			message.append(". The edge references");
			message.append(metadataRefState == ReferenceState.INVALID_REFERENCE ? " missing " : " disabled ");
			message.append("edge ");
			message.append(ReferenceUtils.getElementID(metadataRef));
			message.append(".");
		}
		return message.toString();
	}
}
/*
 *  end class EdgeStub
 */

