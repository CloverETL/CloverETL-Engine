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

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;

/**
 * RoundRobin partition algorithm.
 * 
 * @author david
 * @since  1.3.2005
 *
 */
public class RoundRobinPartition implements PartitionFunction{
    int last;
    int numPorts;
    
    public RoundRobinPartition(){
    }
    
    public void init(int numPartitions,RecordKey partitionKey){
        this.numPorts=numPartitions;
        this.last=-1;
    }
    
	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#preExecute()
	 */
	public void preExecute() throws ComponentNotReadyException {
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#postExecute(org.jetel.graph.TransactionMethod)
	 */
	public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException {
	}

    public int getOutputPort(DataRecord record){
        last=(last+1)%numPorts;
        return last;
    }
    
    public void setGraph(TransformationGraph graph) {
    	// not used here
    }

    public TransformationGraph getGraph() {
    	// not used here
    	return null;
    }

	public int getOutputPort(ByteBuffer directRecord) {
		 last=(last+1)%numPorts;
	     return last;
	}

	public boolean supportsDirectRecord() {
		return true;
	}
    
}