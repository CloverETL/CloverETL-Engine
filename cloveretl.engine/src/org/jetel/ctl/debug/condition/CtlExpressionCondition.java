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
package org.jetel.ctl.debug.condition;

import java.util.ArrayList;
import java.util.List;

import org.jetel.ctl.DebugTransformLangExecutor;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.TLCompiler;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFStartExpression;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.7.2016
 */
public abstract class CtlExpressionCondition implements Condition {

	protected String expression;
	private CLVFStartExpression expressionStart;
	
	public CtlExpressionCondition(String expression) {
		super();
		this.expression = expression;
	}
	
	protected CLVFStartExpression getExpression(DebugTransformLangExecutor executor) throws TransformLangExecutorRuntimeException {
		if (expressionStart == null) {
			TLCompiler compiler = new TLCompiler(executor.getGraph(),
				getMetadata(executor.getInputDataRecords()), getMetadata(executor.getOutputDataRecords()));
			compiler.validateExpression(expression);
			if (compiler.errorCount() > 0) {
				// TODO improve reported error
				StringBuilder sb = new StringBuilder();
				for (ErrorMessage msg : compiler.getDiagnosticMessages()) {
					sb.append(msg.getErrorMessage());
					sb.append("/n");
				}
				throw new TransformLangExecutorRuntimeException(sb.toString());
			}
			expressionStart = compiler.getExpression();
			executor.init(expressionStart);
		}
		return expressionStart;
	}
	
	private DataRecordMetadata[] getMetadata(DataRecord records[]) {
		List<DataRecordMetadata> metadata = new ArrayList<>();
		if (records != null) {
			for (DataRecord record : records) {
				metadata.add(record.getMetadata());
			}
		}
		return metadata.toArray(new DataRecordMetadata[metadata.size()]);
	}
}
