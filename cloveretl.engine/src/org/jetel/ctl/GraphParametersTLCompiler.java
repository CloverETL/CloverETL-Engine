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
package org.jetel.ctl;

import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ProblemReporter;
import org.jetel.ctl.TLCompiler;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CTLTransformUtils;
import org.jetel.util.CTLTransformUtils.CtlAssignmentFinder;
import org.jetel.util.GraphUtils;

/**
 * A custom {@link TLCompiler} that also detects
 * unset required graph parameters.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21. 5. 2014
 */
public class GraphParametersTLCompiler extends TLCompiler {
	
	private static final String TRANSFORM_FUNCTION_NAME = "transform";
	
	private CLVFFunctionDeclaration transformFunction;
	private ProblemReporter reporter = new ProblemReporter();

	public GraphParametersTLCompiler(TransformationGraph graph, DataRecordMetadata[] inMetadata,
			DataRecordMetadata[] outMetadata, String encoding) {
		super(graph, inMetadata, outMetadata, encoding);
	}

	@Override
	public List<ErrorMessage> validate(Reader input) {
		List<ErrorMessage> result = super.validate(input);
		reporter.reset();
		
		if (result.isEmpty()) {
	    	CtlAssignmentFinder ctlAssignmentFinder = new CtlAssignmentFinder(getStart(), inMetadata, outMetadata);
	    	Set<CTLTransformUtils.Field> fields = ctlAssignmentFinder.visit();
	    	CTLTransformUtils.filterFields(fields, true);
	    	List<String> names = findUnsetRequiredParameters(outMetadata, fields);
	    	for (String parameterName : names) {
	    		warn("Required graph parameter " + parameterName + " is not set");
	    	}
		}
		
		result.addAll(reporter.getDiagnosticMessages());

		return result;
	}
	
	/**
	 * 
	 * @param metadata
	 * @param usedFields
	 * @return list of names of required output DataFieldMetadata not used in passed Fields
	 */
	public static List<String> findUnsetRequiredParameters(DataRecordMetadata[] outMetadata, Set<CTLTransformUtils.Field> fields) {
		List<String> names = new ArrayList<String>();
		if (outMetadata == null || fields == null) {
			return null;
		}
		for (int i = 0; i < outMetadata.length; i++) {
			DataRecordMetadata recordMetadata = outMetadata[i];
			if (recordMetadata != null) {
				for (DataFieldMetadata fieldMetadata: recordMetadata) {
					if (Boolean.valueOf(fieldMetadata.getProperty(GraphUtils.REQUIRED_GRAPH_PARAMETER_ATTRIBUTE))) {
						CTLTransformUtils.Field field = new CTLTransformUtils.Field(fieldMetadata.getName(), i, true);
						if (!fields.contains(field)) {
							names.add(fieldMetadata.getName());
						}
					}
				}
			}
		}
		return names;
	}

	/**
	 * @return the transform function
	 */
	private CLVFFunctionDeclaration getTransformFunction() {
		if (transformFunction == null) {
			for (CLVFFunctionDeclaration function: parser.getFunctions().get(TRANSFORM_FUNCTION_NAME)) {
				if (function.getFormalParameters().length == 0 && function.getType() == TLTypePrimitive.INTEGER) {
					transformFunction = function;
				}
			}
		}
		
		return transformFunction;
	}
	
	private void warn(String message) {
		CLVFFunctionDeclaration f = getTransformFunction();
		String hint = null;
		if (f != null) {
			reporter.warn(f.getBegin(), f.getEnd(), message, hint);
		} else {
			reporter.warn(1, 1, 1, 1, message, hint);
		}
	}

}
