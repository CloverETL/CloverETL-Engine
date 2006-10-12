
package org.jetel.component.partition;

import java.util.HashMap;
import java.util.Map;

import org.jetel.data.DataRecord;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;

public class ValuePartition implements PartitionFunction {

    int numPorts;
    HashKey hashKey;
    int portNum = 0;
    Map<HashKey, Integer> keyMap;

    /**
	 * 
	 */
	public ValuePartition() {
	}

	public int getOutputPort(DataRecord record) {
        hashKey.setDataRecord(record);
        if (keyMap.containsKey(hashKey)){
        	return (Integer)keyMap.get(hashKey);
        }else{
        	int port = portNum;
        	keyMap.put(hashKey,portNum < numPorts ? portNum++ : 0);
        	return port;
        }
	}

	public void init(int numPartitions, RecordKey partitionKey) {
	       this.numPorts=numPartitions;
	        hashKey=new HashKey(partitionKey,null);
	        keyMap = new HashMap<HashKey, Integer>(numPorts);
	}

}
