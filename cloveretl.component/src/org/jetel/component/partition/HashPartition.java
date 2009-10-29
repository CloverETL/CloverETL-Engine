package org.jetel.component.partition;

import java.nio.ByteBuffer;

import org.jetel.data.DataRecord;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.graph.TransformationGraph;

/**
 * Partition algorithm based on calculating hash value of
 * specified key. The hash is then split to intervals. Number of
 * intervals is based specified number
 *  
 * 
 * @author david
 * @since  1.3.2005
 */
public class HashPartition implements PartitionFunction{
    int numPorts;
    HashKey hashKey;
    
    public HashPartition(){
    }
    
    public void init(int numPartitions, RecordKey partitionKey){
        this.numPorts=numPartitions;
        hashKey=new HashKey(partitionKey,null);
    }
    
    public int getOutputPort(DataRecord record){
        hashKey.setDataRecord(record);
        //int hash=hashKey.hashCode(); 
        //int value=(hash)&0x0FF;//// take only last 8 bits
        return hashKey.hashCode()%numPorts;
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