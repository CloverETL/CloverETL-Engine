import java.io.File;

import org.jetel.component.DataRecordGenerate;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;


public class FullfillLookup extends DataRecordGenerate {

	private boolean lookupExist;
	
	@Override
	public boolean init() throws ComponentNotReadyException {
		super.init();
		TransformationGraph graph = getGraph();
		TypedProperties graphProperties = graph.getGraphProperties();
		try {
			File lookupFile = new File(FileUtils.getFile(graph.getRuntimeContext().getContextURL(), 
					graphProperties.getStringProperty("lookup_file")));
			lookupExist = lookupFile.exists() && lookupFile.length() > 0;
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
		return true;
	}
	
	@Override
	public int generate(DataRecord[] outputRecords) throws TransformException {
		if (!lookupExist) {
			outputRecords[0].getField(0).setValue("${input_file}");
			return 0;
		}
		outputRecords[1].getField(0).setValue("${lookup_file}");
		return 1;
	}

}
