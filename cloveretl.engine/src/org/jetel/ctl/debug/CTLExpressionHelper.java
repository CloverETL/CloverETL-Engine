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
package org.jetel.ctl.debug;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jetel.ctl.DebugTransformLangExecutor;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.TLCompiler;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFStartExpression;
import org.jetel.ctl.ASTnode.Node;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27.7.2016
 */
public class CTLExpressionHelper {
	
	public static List<Node> compileExpression(String expression, DebugTransformLangExecutor executor, SimpleNode context)
		throws TransformLangExecutorRuntimeException {
		List<Node> result = new ArrayList<>();
		TLCompiler compiler = new TLCompiler(executor.getGraph(),
			getMetadata(executor.getInputDataRecords()),
			getMetadata(executor.getOutputDataRecords()));
		compiler.validateExpressionInContext(expression, context);
		CLVFStartExpression start = compiler.getExpression();
		if (compiler.errorCount() > 0) {
			StringBuilder sb = new StringBuilder();
			for (Iterator<ErrorMessage> it = compiler.getDiagnosticMessages().iterator(); it.hasNext();) {
				sb.append(it.next().toString());
				if (it.hasNext()) {
					sb.append("\n");
				}
			}
			throw new TransformLangExecutorRuntimeException(sb.toString());
		}
		executor.init(start);
		for (int i = 0; i < start.jjtGetNumChildren(); ++i) {
			result.add(start.jjtGetChild(i));
		}
		return result;
	}
	
	private static DataRecordMetadata[] getMetadata(DataRecord records[]) {
		List<DataRecordMetadata> metadata = new ArrayList<>();
		if (records != null) {
			for (DataRecord record : records) {
				metadata.add(record.getMetadata());
			}
		}
		return metadata.toArray(new DataRecordMetadata[metadata.size()]);
	}
}
