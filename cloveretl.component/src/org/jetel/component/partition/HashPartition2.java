
package org.jetel.component.partition;

import java.util.HashMap;
import java.util.Map;

import org.jetel.data.DataRecord;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;

public class HashPartition2 implements PartitionFunction {

    int numPorts;
    HashKey hashKey;
    int portNum = 0;
    Map<Integer, Integer> keyMap;
    
    public HashPartition2(){
    }

    public int getOutputPort(DataRecord record) {
        hashKey.setDataRecord(record);
        int hash=hashKey.hashCode(); 
        if (keyMap.containsKey(hash)){
        	return (Integer)keyMap.get(hash);
        }else{
        	int port = portNum;
        	keyMap.put(hash,portNum < numPorts ? portNum++ : 0);
        	return port;
        }
	}

	public void init(int numPartitions, RecordKey partitionKey) {
	       this.numPorts=numPartitions;
	        hashKey=new HashKey(partitionKey,null);
	        keyMap = new HashMap<Integer, Integer>(numPorts);
	}

}
