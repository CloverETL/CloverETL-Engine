package org.jetel.component.partition;

import java.nio.ByteBuffer;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
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