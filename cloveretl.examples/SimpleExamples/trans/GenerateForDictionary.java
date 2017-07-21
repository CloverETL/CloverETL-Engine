import java.util.Random;

import org.jetel.component.DataRecordGenerate;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;


public class GenerateForDictionary extends DataRecordGenerate {
	
	private static int MAX_VALUE;
	
	Random random = new Random(1);
	int r, min, max;

	public boolean init() throws ComponentNotReadyException {
		MAX_VALUE = getGraph().getGraphProperties().getIntProperty("MAX_VALUE");;
		min = MAX_VALUE;
		max = 0;
		return super.init();
	}
	
	public int generate(DataRecord[] arg0) throws TransformException {
		r = random.nextInt(MAX_VALUE);
		if (r < min) {
			min = r;
		}
		if (r > max) {
			max = r;
		}
		arg0[0].getField(0).setValue(r);
		return 0;
	}

	public void finished() {
		try {
			getGraph().getDictionary().setValue("min", min);
			getGraph().getDictionary().setValue("max", max);
		} catch (ComponentNotReadyException e) {
			throw new RuntimeException(e);
		}
		super.finished();
	}
}
