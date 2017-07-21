import java.nio.ByteBuffer;

import org.jetel.component.partition.DataPartitionFunction;
import org.jetel.data.DataRecord;


public class Partition extends DataPartitionFunction {
	
	private boolean initialized = false;
	private int min, max, value;
	private double median;
	
	private void initInternal(){
		min = (Integer) getGraph().getDictionary().getValue("min");
		max = (Integer) getGraph().getDictionary().getValue("max");
		median = (Double) getGraph().getDictionary().getValue("median");
		initialized = true;
	}

	public int getOutputPort(DataRecord arg0) {
		if (!initialized){
			initInternal();
		}
		value = (Integer) arg0.getField(0).getValue();
		if (value == min) return 0;
		if (value == max) return 3;
		if (value <= median) return 1;
		return 2;
	}

	public int getOutputPort(ByteBuffer arg0) {
		return 0;
	}

	public boolean supportsDirectRecord() {
		return false;
	}

}
