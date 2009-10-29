package org.jetel.component.partition;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
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