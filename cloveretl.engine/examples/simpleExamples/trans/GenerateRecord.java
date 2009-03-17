import org.jetel.component.DataRecordGenerate;
import org.jetel.data.DataRecord;
import org.jetel.exception.TransformException;

public class GenerateRecord extends DataRecordGenerate{

	int counter=0;

	@Override
	public int generate(DataRecord[] outputRecords) throws TransformException {
		System.out.println("Generate Test Called! #"+(counter++));

		outputRecords[0].getField(0).setValue("a Value");
		outputRecords[0].getField(3).setValue(1);
		return ALL;
	}
		

}
