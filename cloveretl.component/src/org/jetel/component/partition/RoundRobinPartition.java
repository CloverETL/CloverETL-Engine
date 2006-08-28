package org.jetel.component.partition;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;

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
    
}