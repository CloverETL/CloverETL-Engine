import java.util.Random;

import org.jetel.component.DataRecordGenerate;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;

public class GenerateRecord extends DataRecordGenerate{

	int counter=0;
	Random r = new Random(10);
	Lookup lookup;
	DataRecord keyRecord;
	Sequence sequence;
	
	public boolean init() throws ComponentNotReadyException {
		LookupTable lTable = getLookupTable("LookupTable0");
		lTable.init();
		DataRecordMetadata keyMetadata = lTable.getKeyMetadata();
		keyRecord = DataRecordFactory.newRecord(keyMetadata);
		keyRecord.init();
		lookup = lTable.createLookup(new RecordKey(keyMetadata.getFieldNamesArray(), keyMetadata), keyRecord);
		
		sequence = getGraph().getSequence("Sequence0");
		return super.init();
	}
	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		sequence.resetValue();
	}
	
	public int generate(DataRecord[] outputRecords) throws TransformException {

		keyRecord.getField(0).setValue(r.nextInt(11));
		lookup.seek();
		if (!lookup.hasNext()) {
			return SKIP;
		}
		outputRecords[0].getField(5).setValue(lookup.next().getField(1));
		outputRecords[0].getField(6).setValue(keyRecord.getField(0));
		outputRecords[0].getField(0).setValue("a Value");
		outputRecords[0].getField(3).setValue(r.nextInt());
		outputRecords[0].getField(4).setValue(sequence.nextValueInt());
		return ALL;
	}

	public String getMessage() {
		return "Lookup value doesn't exist for key: " + keyRecord.getField(0);
	}
}
