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
package org.jetel.graph.parameter;

import java.text.MessageFormat;
import java.util.Date;

import org.jetel.component.TransformFactory;
import org.jetel.component.TransformUtils;
import org.jetel.ctl.ITLCompilerFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.TransformException;
import org.jetel.graph.GraphParameter;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.formatter.DateFormatter;
import org.jetel.util.formatter.DateFormatterFactory;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;

/**
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29. 4. 2014
 */
public class GraphParameterDynamicValueProvider {
	
	private static ITLCompilerFactory COMPILER_FACTORY;
	
	private static interface TransformationGraphProvider {
		TransformationGraph getGraph();
	}
	
	private final TransformationGraphProvider graphProvider;
	private final String parameterName;
	private final String transformCode;
	private final TransformFactory<GraphParameterValueFunction> factory;
	
	private boolean initialized;
	private GraphParameterValueFunction transform;
	private boolean recursionFlag;
	
	private String value = null;

	/**
	 * CLO-5564:
	 * Used for conversion of Date to String.
	 */
	private DateFormatter dateFormatter;
	
	private GraphParameterDynamicValueProvider(TransformationGraphProvider graphProvider, String parameterName, String transformCode, TransformFactory<GraphParameterValueFunction> factory) {
		super();
		this.graphProvider = graphProvider;
		this.parameterName = parameterName;
		this.transformCode = transformCode;
		this.factory = factory;
	}
	
	private static TransformFactory<GraphParameterValueFunction> createTransformFactory() {
		TransformFactory<GraphParameterValueFunction> factory = TransformFactory.createTransformFactory(GraphParameterValueFunctionDescriptor.newInstance());
		if (COMPILER_FACTORY != null) {
			factory.setCompilerFactory(COMPILER_FACTORY);
		}
		return factory;
	}
	
	public static GraphParameterDynamicValueProvider create(final GraphParameter graphParameter, String transformCode) {
		TransformationGraphProvider transformationGraphProvider = new TransformationGraphProvider() {
			@Override
			public TransformationGraph getGraph() {
				return graphParameter.getParentGraph();
			}
		};
		
		TransformFactory<GraphParameterValueFunction> factory = createTransformFactory();

		return new GraphParameterDynamicValueProvider(transformationGraphProvider, graphParameter.getName(), transformCode, factory);
	}
	
	public static GraphParameterDynamicValueProvider create(final TransformationGraph graph, String parameterName, String transformCode) {
		TransformationGraphProvider transformationGraphProvider = new TransformationGraphProvider() {
			@Override
			public TransformationGraph getGraph() {
				return graph;
			}
		};
		TransformFactory<GraphParameterValueFunction> factory = createTransformFactory();

		return new GraphParameterDynamicValueProvider(transformationGraphProvider, parameterName, transformCode, factory);
	}
	
	private Node createNodeForTransformation() {
		Node node = new Node(StringUtils.normalizeName("__PARAM_TRANSFORM_NODE_" + parameterName), graphProvider.getGraph()) {
			@Override
			protected Result execute() throws Exception {
				return null;
			}
		};
		return node;
	}
	
	public synchronized void init() throws ComponentNotReadyException {
		if (initialized) {
			return;
		}
		Node node = createNodeForTransformation();
		factory.setComponent(node);
		String jobUrl = graphProvider.getGraph().getRuntimeContext().getJobUrl();
		if (jobUrl != null) {
			factory.setTransformSourceId(TransformUtils.createCTLSourceId(jobUrl, TransformUtils.GRAPH_PARAMETER_NAME_PARAM, parameterName));
		}
		PropertyRefResolver propertyRefResolver = graphProvider.getGraph().getPropertyRefResolver();
		String resolvedCode = propertyRefResolver.resolveRef(transformCode, RefResFlag.SPEC_CHARACTERS_OFF);
		factory.setTransform(resolvedCode);
		transform = factory.createTransform();
		if (transform == null) {
			throw new IllegalStateException("Dynamic value transform wasn't created");
		}
		transform.init();
		
		initialized = true;
	}
	
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		return factory.checkConfig(status);
	}
	
	private DateFormatter getDateFormatter() {
		if (dateFormatter == null) {
			dateFormatter = DateFormatterFactory.getFormatter(Defaults.DEFAULT_DATETIME_FORMAT);
		}
		return dateFormatter;
	}
	
	private String toString(Object value) {
		if (value instanceof Date) {
			return getDateFormatter().format((Date) value);
		}
		return String.valueOf(value);
	}
	
	public synchronized String getValue() {
		try {
			init();
		} catch (ComponentNotReadyException e1) {
			throw new JetelRuntimeException("Cannot initialize dynamic parameter", e1);
		}
		
		// CLO-6809: cache the value
		if (value == null) {
			try {
				if (recursionFlag) {
					throw new JetelRuntimeException(MessageFormat.format(
							"Infinite recursion detected when resolving dynamic value for graph parameter ''{0}''",
							parameterName));
				}
				
				recursionFlag = true;
				value = toString(transform.getValue());
			} catch (TransformException e) {
				throw new JetelRuntimeException("Cannot get parameter value", e);
			} finally {
				recursionFlag = false;
			}
		}
		
		return value;
	}
	
	public String getTransformCode() {
		return transformCode;
	}

	public static void setCompilerFactory(ITLCompilerFactory compilerFactory) {
		GraphParameterDynamicValueProvider.COMPILER_FACTORY = compilerFactory;
	}
	
}
