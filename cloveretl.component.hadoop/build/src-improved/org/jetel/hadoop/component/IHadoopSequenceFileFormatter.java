package org.jetel.hadoop.component;

import org.jetel.data.formatter.Formatter;

public interface IHadoopSequenceFileFormatter extends Formatter {

	public void setKeyValueFields(String keyFieldName, String valueFieldName);
	
}
