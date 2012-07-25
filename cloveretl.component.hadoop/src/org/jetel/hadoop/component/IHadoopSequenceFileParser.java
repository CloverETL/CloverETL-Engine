package org.jetel.hadoop.component;

import org.jetel.data.parser.Parser;
import org.jetel.graph.TransformationGraph;
import org.jetel.hadoop.connection.IHadoopConnection;
import org.jetel.metadata.DataRecordMetadata;

public interface IHadoopSequenceFileParser extends Parser {

	public void setKeyValueFields(String keyFieldName, String valueFieldName);
	public void setMetadata(DataRecordMetadata metadata);
	public void setHadoopConnection(IHadoopConnection conn);
	public void setGraph(TransformationGraph graph);
	
}
