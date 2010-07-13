import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.jetel.component.CustomizedRecordTransform;
import org.jetel.component.DataRecordTransform;
import org.jetel.data.DataRecord;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;


public class customizedTransformExample extends DataRecordTransform {
	
	CustomizedRecordTransform transformation;
	
	public boolean init(Properties arg0, DataRecordMetadata[] arg1, DataRecordMetadata[] arg2) throws ComponentNotReadyException {
		transformation = new CustomizedRecordTransform(LogFactory.getLog(customizedTransformExample.class));
		transformation.addFieldToFieldRule("*.*", "*.*");
		String seqName = arg0.containsKey("seq_name") ? arg0.getProperty("seq_name") : "Sequence0";
		Sequence sequence = getGraph().getSequence(seqName);
		sequence.resetValue();
		transformation.addSequenceToFieldRule("0.no", sequence);
		transformation.setGraph(this.getGraph());
		return transformation.init(arg0, arg1, arg2);
	}

	public int transform(DataRecord[] arg0, DataRecord[] arg1)
			throws TransformException {
		return transformation.transform(arg0, arg1);
	}

}
