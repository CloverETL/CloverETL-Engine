package org.jetel.hadoop.component;

import org.jetel.data.formatter.Formatter;
import org.jetel.graph.TransformationGraph;
import org.jetel.hadoop.connection.IHadoopConnection;

public interface IHadoopSequenceFileFormatter extends Formatter {

	public void setKeyValueFields(String keyFieldName, String valueFieldName);
	public void setHadoopConnection(IHadoopConnection conn);
	public void setGraph(TransformationGraph graph);
	
}
