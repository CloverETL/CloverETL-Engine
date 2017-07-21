import java.io.File;

import org.jetel.component.DataRecordGenerate;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;


public class CreateHeader extends DataRecordGenerate {

	private boolean outputExist;
	private double sample_size;
	private String sampling_field;
	
	@Override
	public boolean init() throws ComponentNotReadyException {
		super.init();
		TransformationGraph graph = getGraph();
		TypedProperties graphProperties = graph.getGraphProperties();
		try {
			File outputFile = new File(FileUtils.getFile(graph.getRuntimeContext().getContextURL(), 
					graphProperties.getStringProperty("comparison_file")));
			outputExist = outputFile.exists();
			sample_size = graphProperties.getDoubleProperty("sample_size");
			sampling_field = graphProperties.getStringProperty("sampling_field");
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
		return true;
	}
	
	@Override
	public int generate(DataRecord[] outputRecords) throws TransformException {
		if (!outputExist) {
			outputRecords[0].getField(0).setValue("<html>\n<body>\n<h1>Comparison of different sampling methods</h1>\n" +
					"<p>defined sample size ratio:  " + sample_size + "</p>\n" +
				"<table border=\"1\">\n<tr align=\"center\">\n\t<th>sampling field (" + sampling_field + ") value</th>\n" +
				"\t<th colspan=3>simple sampling</th>\n\t<th colspan=3>systematic sampling</th>\n\t<th colspan=3>stratified sampling</th>\n" +
				"\t<th colspan=3>pps sampling</th>\n</tr>");
			return 0;
		}
		return SKIP;
	}

}
