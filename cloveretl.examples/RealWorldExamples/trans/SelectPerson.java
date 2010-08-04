import java.nio.CharBuffer;

import org.jetel.data.DataRecord;

import com.opensys.cloveretl.data.parser.MultiLevelSelector;
import com.opensys.cloveretl.data.parser.PrefixMultiLevelSelector;


public class SelectPerson extends PrefixMultiLevelSelector {
	
	/**
	 * This counter identify each person; person, his/her spouse and children have the same number
	 */
	int counter = 0;
	
	@Override
	public int choose(CharBuffer data, DataRecord[] lastRecords) {
		int port =  super.choose(data, lastRecords);
		if (port == MultiLevelSelector.UNKNOWN_DATA) {//person record has no prefix, so parent selector returns MultiLevelSelector.UNKNOWN_DATA
			port = 0;
			counter++;//next person
		}
		return port;
	}
	
	@Override
	public void postProcess(int metadataIndex, DataRecord[] records) {
		//set person id for downstream joining
		records[metadataIndex].getField("id").setValue(counter);
	}
}
