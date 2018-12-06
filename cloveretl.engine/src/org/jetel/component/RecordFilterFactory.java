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
import org.jetel.component.TransformLanguageDetector.TransformLanguage;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ITLCompiler;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.CompoundException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.compile.ClassLoaderUtils;

/**
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 10.8.2010
 */
public class RecordFilterFactory {

	private static final DataRecordMetadata NO_METADATA[] = new DataRecordMetadata[0];
	
	public static RecordFilter createFilter(String className, Node node) throws ComponentNotReadyException {
		return ClassLoaderUtils.loadClassInstance(RecordFilter.class, className, node);
	}
	public static RecordFilter createFilter(String filterExpression, DataRecordMetadata metadata, TransformationGraph graph, String id, String attributeName, String sourceId, Log logger) throws ComponentNotReadyException {
		return createFilter(filterExpression, new DataRecordMetadata[] { metadata }, graph, id, attributeName, sourceId, logger);
	}

	public static RecordsFilter createFilter(String filterExpression, DataRecordMetadata[] metadata, TransformationGraph graph, String id, String attributeName, String sourceId) throws ComponentNotReadyException {
		return createFilter(filterExpression, metadata, graph, id, attributeName, sourceId, null);
	}
	
	public static RecordsFilter createFilter(String filterExpression, DataRecordMetadata[] metadata, TransformationGraph graph, String id, String attributeName, String sourceId, Log logger) throws ComponentNotReadyException {
		return commonBuilder(filterExpression, graph, id, sourceId, logger).setMetadata(metadata).setAttributeName(attributeName).build();
	}

	public static Builder builder(String filterExpression, DataRecordMetadata metadata, TransformationGraph graph, String id, String sourceId, Log logger) throws ComponentNotReadyException {
		return commonBuilder(filterExpression, graph, id, sourceId, logger).setMetadata(metadata);
	}
	
	private static Builder commonBuilder(String filterExpression, TransformationGraph graph, String id, String sourceId, Log logger) {
		return new Builder().setFilterExpression(filterExpression).setGraph(graph).setId(id).setSourceId(sourceId).setLogger(logger);
	}

	public static class Builder {
		
		private String filterExpression;
		private TransformLanguage defaultLanguage;
		private DataRecordMetadata[] metadata;
		private TransformationGraph graph;
		private String id; 
		private String attributeName; 
		private String sourceId;
		private Log logger;
		
		public RecordsFilter build() throws ComponentNotReadyException {
			RecordsFilter filter;
			
			if (filterExpression.contains(org.jetel.ctl.TransformLangExecutor.CTL_TRANSFORM_CODE_ID) || (defaultLanguage == TransformLanguage.CTL2)) {
				// new CTL initialization
				ITLCompiler compiler = TLCompilerFactory.createCompiler(graph, metadata, NO_METADATA, "UTF-8");
				
				compiler.setSourceId(sourceId != null ? sourceId : createFilterExpressionSourceId(graph, id, attributeName));
		    	
				List<ErrorMessage> msgs = compiler.compileExpression(filterExpression, CTLRecordFilter.class, id, CTLRecordFilterAdapter.ISVALID_FUNCTION_NAME, boolean.class);
		    	if (compiler.errorCount() > 0) {
		    		ComponentNotReadyException[] errors = new ComponentNotReadyException[msgs.size()];
		    		for (int i = 0; i < msgs.size(); i++) {
		    			errors[i] = new ComponentNotReadyException(msgs.get(i).toString());
			    		if (logger != null) {
			    			logger.error(msgs.get(i).toString());
			    		}
		    		}
		    		CompoundException compoundException = new CompoundException(errors);
		    		throw new ComponentNotReadyException("CTL code compilation finished with " + compiler.errorCount() + " errors", compoundException);
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
	        	throw new JetelRuntimeException("CTL1 is not a supported language any more, please convert your code to CTL2.");
			}
			
			return filter;
		}

		public Builder setFilterExpression(String filterExpression) {
			this.filterExpression = filterExpression;
			return this;
		}
		
		public Builder setDefaultLanguage(TransformLanguage defaultLanguage) {
			this.defaultLanguage = defaultLanguage;
			return this;
		}
		
		/**
		 * Sets CTL2 as the default transformation language.
		 * Filter expressions without a prefix determining the language will be treated as CTL2.
		 * 
		 * @return this builder
		 */
		public Builder setDefaultLanguageCTL2() {
			this.defaultLanguage = TransformLanguage.CTL2;
			return this;
		}
		
		public Builder setMetadata(DataRecordMetadata[] metadata) {
			this.metadata = metadata;
			return this;
		}
		
		public Builder setMetadata(DataRecordMetadata metadata) {
			this.metadata = new DataRecordMetadata[] { metadata };
			return this;
		}
		
		public Builder setGraph(TransformationGraph graph) {
			this.graph = graph;
			return this;
		}
		
		public Builder setId(String id) {
			this.id = id;
			return this;
		}
		
		public Builder setAttributeName(String attributeName) {
			this.attributeName = attributeName;
			return this;
		}
		
		public Builder setSourceId(String sourceId) {
			this.sourceId = sourceId;
			return this;
		}
		
		public Builder setLogger(Log logger) {
			this.logger = logger;
			return this;
		}
		
	}
	
	private static String createFilterExpressionSourceId(TransformationGraph graph, String graphElemId, String attributeName) {
		if (graphElemId != null && attributeName != null && graph != null && graph.getRuntimeContext() != null) {
			String jobUrl = graph.getRuntimeContext().getJobUrl();
			if (jobUrl != null) {
				return TransformUtils.createCTLSourceId(jobUrl, TransformUtils.COMPONENT_ID_PARAM, graphElemId,
						TransformUtils.PROPERTY_NAME_PARAM, attributeName);
			}
		}
		return null;
	}
}
