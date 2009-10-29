/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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
package org.jetel.component.partition;

import java.nio.ByteBuffer;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.primitive.Numeric;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.lookup.RangeLookupTable;

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
    
    /* (non-Javadoc)
     * @see org.jetel.component.partition.PartitionFunction#init(int, org.jetel.data.RecordKey)
     */
    public void init(int numPartitions, RecordKey partitionKey) throws ComponentNotReadyException{
        lookupTable.init();
        lookup = lookupTable.createLookup(partitionKey);
    }
    
    /* (non-Javadoc)
     * @see org.jetel.component.partition.PartitionFunction#getOutputPort(org.jetel.data.DataRecord)
     */
    public int getOutputPort(DataRecord record){
    	lookup.seek(record);
    	portRecord = lookup.hasNext() ? lookup.next() : null;
    	return portRecord != null ? ((Numeric)portRecord.getField(portField)).getInt() :
    		rejectedPort;
    }
    
    public void setGraph(TransformationGraph graph) {
    	// not used here
    }

    public TransformationGraph getGraph() {
    	// not used here
    	return null;
    }

	public int getOutputPort(ByteBuffer directRecord) {
		throw new UnsupportedOperationException();
	}

	public boolean supportsDirectRecord() {
		return false;
	}
}