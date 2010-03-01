import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.jetel.component.DataRecordTransform;
import org.jetel.data.DataRecord;
import org.jetel.data.DateDataField;
import org.jetel.data.primitive.Numeric;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataFieldMetadata;


public class StatNormalizer extends DataRecordTransform {

	private static DateFormat DATE_FORMAT;

	public boolean init() throws ComponentNotReadyException {
		DATE_FORMAT = new SimpleDateFormat(getGraph().getGraphProperties().getStringProperty("DATE_FORMAT"));
		return super.init();
	}
	
	public int transform(DataRecord[] arg0, DataRecord[] arg1)
			throws TransformException {
		defaultTransform(arg0, arg1);
		if (arg0[0].getField(1).equals(String.valueOf(DataFieldMetadata.DATE_TYPE))) {
			for (int i = 2; i < 5; i++){
				arg1[0].getField(i).setValue(DATE_FORMAT.format(new Date(((Numeric)arg0[0].getField(i)).getLong())));
			}
		}
		return 0;
	}

}
