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

import java.util.List;

import org.apache.commons.logging.Log;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ITLCompiler;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.ASTnode.CLVFStartExpression;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 10.8.2010
 */
public class RecordFilterFactory {

	public static RecordFilter createFilter(String filterExpression, DataRecordMetadata metadata, TransformationGraph graph, String id, Log logger) throws ComponentNotReadyException {
		RecordFilter filter;
		
		if (filterExpression.contains(org.jetel.ctl.TransformLangExecutor.CTL_TRANSFORM_CODE_ID)) {
			// new CTL initialization
			DataRecordMetadata[] inputMetadata = new DataRecordMetadata[] { metadata };
			DataRecordMetadata[] outputMetadata = new DataRecordMetadata[] { metadata };
			ITLCompiler compiler = TLCompilerFactory.createCompiler(graph, inputMetadata, outputMetadata, "UTF-8");
	    	
			List<ErrorMessage> msgs = compiler.compileExpression(filterExpression, CTLRecordFilter.class, id, CTLRecordFilterAdapter.ISVALID_FUNCTION_NAME, boolean.class);
	    	if (compiler.errorCount() > 0) {
	    		if (logger != null) {
		    		for (ErrorMessage msg : msgs) {
		    			logger.error(msg.toString());
		    		}
	    		}
	    		throw new ComponentNotReadyException("CTL code compilation finished with " + compiler.errorCount() + " errors");
	    	}
	    	Object ret = compiler.getCompiledCode();
	    	if (ret instanceof org.jetel.ctl.TransformLangExecutor) {
	    		// setup interpreted runtime
	    		filter = new CTLRecordFilterAdapter((org.jetel.ctl.TransformLangExecutor) ret, logger);
	    	} else if (ret instanceof CTLRecordFilter){
	    		filter = (CTLRecordFilter) ret;
	    	} else {
	    		// this should never happen as compiler always generates correct interface
	    		throw new ComponentNotReadyException("Invalid type of record transformation");
	    	}
	    	// set graph instance to transformation (if CTL it can access lookups etc.)
	    	filter.setGraph(graph);
	    	
	    	// initialize transformation
	    	filter.init();
		} else {
			// old TL initialization
			TransformLangParser parser=new TransformLangParser(metadata, filterExpression);
			try {
				  final CLVFStartExpression recordFilter = parser.StartExpression();
				  filter = new RecordFilterTL(recordFilter);
				  if (graph != null ){
					  filter.setGraph(graph);
				  }
				  filter.init();
			} catch (ParseException ex) {
                throw new ComponentNotReadyException("Parser error when parsing expression: "+ex.getMessage());
            } catch (Exception e) {
				throw new ComponentNotReadyException("Error when initializing expression: "+e.getMessage());
			}
		}
		
		return filter;
	}
	
}
