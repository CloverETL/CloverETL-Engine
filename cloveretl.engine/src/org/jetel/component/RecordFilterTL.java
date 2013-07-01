/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.component;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
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
	
	@Override
	public void init() throws ComponentNotReadyException {
		filterExpression.init();
	}

	@Override
	public boolean isValid(DataRecord record) throws TransformException {
		input[0] = record;
		return isValid(input);
	}
	
	@Override
	public boolean isValid(DataRecord[] records) {
		executor.setInputRecords(records);
		executor.visit(filterExpression, null);
		return executor.getResult() == TLBooleanValue.TRUE;
	}

	@Override
	public void setGraph(TransformationGraph graph) {
		executor.setGraph(graph);
	}

}
