package org.jetel.component;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.TransformLangExecutor;
import org.jetel.interpreter.ASTnode.CLVFStartExpression;
import org.jetel.interpreter.data.TLBooleanValue;

/**
 * Adapter for old TL language implementation
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 *
 */
public class RecordFilterTL implements RecordFilter {

	private final CLVFStartExpression filterExpression;
	private final TransformLangExecutor executor = new TransformLangExecutor();
	private final DataRecord[] input = new DataRecord[1];
	
	public RecordFilterTL(CLVFStartExpression filterExpression) {
		this.filterExpression = filterExpression;
		
	}
	
	public void init() throws ComponentNotReadyException {
		filterExpression.init();
	}

	public boolean isValid(DataRecord record) {
		input[0] = record;
		executor.setInputRecords(input);
		executor.visit(filterExpression, null);
		return executor.getResult() == TLBooleanValue.TRUE;
	}

	public void setGraph(TransformationGraph graph) {
		// not used
	}

}
