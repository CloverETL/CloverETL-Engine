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
package org.jetel.component.partition;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.primitive.Numeric;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.lookup.RangeLookupTable;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 * This class uses range lookup table for data partition
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Sep 24, 2007
 *
 */
public class RangePartition implements PartitionFunction{
	
	public final static int NONEXISTENT_REJECTED_PORT = -1; 
	
	RangeLookupTable lookupTable;
	Lookup lookup;
	int portField;
	DataRecord portRecord;
	int rejectedPort;
	
    /**
     * Creates new RangePartition object from given range lookup table
     * 
     * @param lookup range lookup table with interval's definition
     * @param portField number of field with output port number
     * @param rejectedPort number for records without pair in lookup table
     */
    public RangePartition(RangeLookupTable lookup, int portField, int rejectedPort){
        this.lookupTable = lookup;
        this.portField = portField;
        this.rejectedPort = rejectedPort;
    }
    
    /**
     * Creates new RangePartition object with default rejectedPort = NONEXISTENT_REJECTED_PORT (-1)
     * 
     * @param lookup range lookup table with interval's definition
     * @param portField number of field with output port number
     */
    public RangePartition(RangeLookupTable lookup, int portField){
    	this(lookup, portField, NONEXISTENT_REJECTED_PORT);
    }
    
    @Override
    @Deprecated
	public void init(int numPartitions, RecordKey partitionKey) throws ComponentNotReadyException{
    	init(numPartitions, partitionKey, null, null);
    }
    
    @Override
    public void init(int numPartitions, RecordKey partitionKey, Properties parameters, DataRecordMetadata metadata) throws ComponentNotReadyException {
        lookupTable.init();
        lookup = lookupTable.createLookup(partitionKey);
    }
    
	@Override
	public void preExecute() throws ComponentNotReadyException {
		lookupTable.preExecute();
	}
	
	@Override
	public void postExecute() throws ComponentNotReadyException {
		lookupTable.postExecute();
	}

    @Override
	public int getOutputPort(DataRecord record){
    	lookup.seek(record);
    	portRecord = lookup.hasNext() ? lookup.next() : null;
    	return portRecord != null ? ((Numeric)portRecord.getField(portField)).getInt() :
    		rejectedPort;
    }
    
	@Override
	public int getOutputPortOnError(Exception exception, DataRecord record) throws TransformException {
		// by default just throw the exception that caused the error
		throw new TransformException("Partitioning failed!", exception);
	}

    /**
	 * Use setNode method.
	 */
    @Deprecated
    public void setGraph(TransformationGraph graph) {
    	// not used here
    }

    @Override
	public TransformationGraph getGraph() {
    	// not used here
    	return null;
    }

    @Override
	public void setNode(Node node) {
    	// not used here
    }
    
    @Override
	public Node getNode() {
    	// not used here
    	return null;
    }
    
	@Override
	public int getOutputPort(ByteBuffer directRecord) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getOutputPort(CloverBuffer directRecord) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getOutputPortOnError(Exception exception, ByteBuffer directRecord) throws TransformException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getOutputPortOnError(Exception exception, CloverBuffer directRecord) throws TransformException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean supportsDirectRecord() {
		return false;
	}

	@Override
	public String getMessage() {
		return null;
	}

	/**
	 * @deprecated Use {@link #postExecute()} method.
	 */
	@Deprecated
	@Override
	public void finished(){
		// do nothing by default
	}

	/**
	 * @deprecated Use {@link #preExecute()} method.
	 */
	@Deprecated
	@Override
	public void reset() {
		// do nothing by default
	}

}