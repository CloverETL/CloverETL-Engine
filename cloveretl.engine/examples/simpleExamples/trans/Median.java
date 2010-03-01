import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jetel.component.rollup.DataRecordRollup;
import org.jetel.data.DataRecord;
import org.jetel.data.IntegerDataField;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;


public class Median extends DataRecordRollup {
	
	private List elements = new ArrayList();

	public boolean finishGroup(DataRecord arg0, DataRecord arg1)
			throws TransformException {
		return true;
	}

	public void initGroup(DataRecord arg0, DataRecord arg1)
			throws TransformException {
	}

	public int transform(int arg0, DataRecord arg1, DataRecord arg2,
			DataRecord[] arg3) throws TransformException {
		Collections.sort(elements);
		int index = elements.size() / 2; 
		double median;
		if (elements.size() % 2 == 0) {
			median = ((Integer)elements.get(index - 1) + (Integer)elements.get(index))/2; 
		}else{
			median = (Integer)elements.get(index);
		}
		try {
			getGraph().getDictionary().setValue("median", median);
		} catch (ComponentNotReadyException e) {
			throw new TransformException("Error when setting dictionary value", e);
		}
		return SKIP;
	}

	public boolean updateGroup(DataRecord arg0, DataRecord arg1)
			throws TransformException {
		elements.add(((IntegerDataField)arg0.getField(0)).getInt());
		return true;
	}

	public int updateTransform(int arg0, DataRecord arg1, DataRecord arg2,
			DataRecord[] arg3) throws TransformException {
		if (arg0 > 0) return SKIP;
		arg3[0].copyFrom(arg1);
		return ALL;
	}

}
