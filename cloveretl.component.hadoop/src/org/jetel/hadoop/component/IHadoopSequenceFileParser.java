package org.jetel.hadoop.component;

import org.jetel.data.parser.Parser;
import org.jetel.metadata.DataRecordMetadata;

public interface IHadoopSequenceFileParser extends Parser {

	public void setKeyValueFields(String keyFieldName, String valueFieldName);
	public void setMetadata(DataRecordMetadata metadata);
	
}
