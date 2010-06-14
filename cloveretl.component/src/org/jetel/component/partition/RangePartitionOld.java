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
import java.util.Arrays;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;

/**
 * Partition algorithm which compares current key value with set of
 * intervals (defined as a set of interval upper limits - inclusive).
 * 
 * @author david
 * @since  5.3.2005
 *
 */
public class RangePartitionOld implements PartitionFunction{
    int numPorts;
    DataField[] boundaries;
    int[] keyFields;
    String[] boundariesStr;
    
    /**
     * @param boundariesStr array of strings containing definitions of ranges boundaries
     * @param record	DataRecord with the same structure as the one which will be used
     * for determining output port.
     */
    public RangePartitionOld(String[] boundariesStr){
        this.boundariesStr=boundariesStr;
    }
    
    public void init(int numPartitions, RecordKey partitionKey){
        this.numPorts=numPartitions;
        keyFields=partitionKey.getKeyFields();

    }
    
	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#preExecute()
	 */
	public void preExecute() throws ComponentNotReadyException {
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#postExecute(org.jetel.graph.TransactionMethod)
	 */
	public void postExecute() throws ComponentNotReadyException {
	}

    public int getOutputPort(DataRecord record){
        // create boundaries the first time this method is called
        if (boundaries==null){
            boundaries = new DataField[boundariesStr.length];
	        for (int i=0;i<boundaries.length;i++){
	            boundaries[i]=record.getField(keyFields[0]).duplicate();
	            boundaries[i].fromString(boundariesStr[i]);
	        }
	        Arrays.sort(boundaries);
        }
        
        // use sequential search if number of boundries is small
        if (boundaries.length <= 6) {
            for (int i = 0; i < numPorts && i < boundaries.length; i++) {
                // current boundary is upper limit inclusive
                if (record.getField(keyFields[0]).compareTo(boundaries[i]) <= 0) {
                    return i;
                }
            }
            return numPorts - 1;
        } else {
            int index = Arrays.binarySearch(boundaries, record
                    .getField(keyFields[0]));
            // DEBUG
            // System.out.println("Index partition: "+index+" value "+record
            //         .getField(keyFields[0]).toString());
            // DEBUG END
            if (index>=0){
                return index;
            }else{
                return (index*-1)-1;
            }
        }
    }
    
    /**
	 * Use setNode method.
	 */
    @Deprecated
    public void setGraph(TransformationGraph graph) {
    	// not used here
    }

    public TransformationGraph getGraph() {
    	// not used here
    	return null;
    }

    /* (non-Javadoc)
     * @see org.jetel.component.partition.PartitionFunction#setNode(org.jetel.graph.Node)
     */
    public void setNode(Node node) {
    }
    
    /* (non-Javadoc)
     * @see org.jetel.component.partition.PartitionFunction#getNode()
     */
    public Node getNode() {
    	return null;
    }
    
	public int getOutputPort(ByteBuffer directRecord) {
		throw new UnsupportedOperationException();
	}

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