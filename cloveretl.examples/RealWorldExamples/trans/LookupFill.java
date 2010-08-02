import org.jetel.component.DataRecordTransform;
import org.jetel.data.DataRecord;
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;


public class LookupFill extends DataRecordTransform {
	
	LookupTable lookup;

	@Override
	public boolean init() throws ComponentNotReadyException {
		lookup = getLookupTable("LookupTable0");
		return super.init();
	}
	
	@Override
	public int transform(DataRecord[] arg0, DataRecord[] arg1)
			throws TransformException {
		lookup.put(arg0[0]);
		return SKIP;
	}

}
