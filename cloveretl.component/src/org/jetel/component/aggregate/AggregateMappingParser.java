/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.component.aggregate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.component.Aggregate;
import org.jetel.data.RecordKey;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Parses the aggregation mapping.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Apr 23, 2007
 */
public class AggregateMappingParser {
	private static final String MAPPING_LEFT_SIDE_REGEX = "^[\\w]*[ ]*" + Aggregate.ASSIGN_SIGN + "[ ]*";
	// regular expression matching a correct aggregate function mapping
	private static final String MAPPING_FUNCTION_REGEX = "[\\w ]*\\([\\w ]*\\)$";
	// regular expression matching a correct field mapping
	private static final String MAPPING_FIELD_REGEX = "\\$[\\w ]*$";

	private static final Pattern functionPattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_FUNCTION_REGEX);
	private static final Pattern fieldPattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_FIELD_REGEX);
	
	// set of already used output fields, used to detect multiple uses of the same output field
	private Set<String> usedOutputFields = new HashSet<String>(); 
	// fields of the aggregation key
	private Set<String> keyFields;
	// aggregation function registry
	private FunctionRegistry registry;

	private DataRecordMetadata inMetadata;
	private DataRecordMetadata outMetadata;
	
	private List<FunctionMapping> functionMapping = new ArrayList<FunctionMapping>();
	private List<FieldMapping> fieldMapping = new ArrayList<FieldMapping>();
	
	/**
	 * 
	 * Allocates a new <tt>AggregateMappingParser</tt> object.
	 *
	 * @param mapping aggregation mapping.
	 * @param recordKey aggregation key.
	 * @param registry aggregation function registry.
	 * @param inMetadata input metadata.
	 * @param outMetadata output metadata.
	 * @throws AggregationException
	 */
	public AggregateMappingParser(String[] mapping, 
			RecordKey recordKey, FunctionRegistry registry, 
			DataRecordMetadata inMetadata, DataRecordMetadata outMetadata) 
	throws AggregationException {
		this.inMetadata = inMetadata;
		this.outMetadata = outMetadata;
		this.registry = registry;
		
		keyFields = new HashSet<String>();
		int[] keyFieldIndices = recordKey.getKeyFields();
		for (int i : keyFieldIndices) {
			keyFields.add(inMetadata.getField(i).getName());
		}

		parseMapping(mapping);
	}
	
	/**
	 * Parses the mapping.
	 * 
	 * @param mapping
	 * @throws AggregationException
	 */
	private void parseMapping(String[] mapping) throws AggregationException {
		for (String expression : mapping) {
			String expr2 = expression.trim();
			Matcher functionMatcher = functionPattern.matcher(expr2);
			Matcher fieldMatcher = fieldPattern.matcher(expr2);

			try {
				if (functionMatcher.matches()) {
					// mapping contains an aggregate function

					parseFunction(expr2);
				} else if (fieldMatcher.matches()) {
					// mapping contains the name of a field in input

					parseFieldMapping(expr2);
				} else {
					throw new AggregationException("Invalid mapping format");
				}
			} catch (AggregationException e) {
				throw new AggregationException("Invalid aggregation mapping " + expr2 
						+ " :\n" + e.getMessage(), e);
			}
		}
	}
	
	/**
	 * Parses a function mapping.
	 * 
	 * @param expression
	 * @throws AggregationException
	 */
	private void parseFunction(String expression) throws AggregationException {
		String[] parsedExpression = expression.split(Aggregate.ASSIGN_SIGN);
		String function = parsedExpression[1].trim();
		String outputField = parsedExpression[0].trim();

		// parse the aggregate function name
		int parenthesesIndex = function.indexOf("(");
		String functionName = function.substring(0, parenthesesIndex).trim().toLowerCase();

		// parse the input field name
		String inputField = function.substring(parenthesesIndex + 1, function.length() - 1).trim();

		if (!usedOutputFields.contains(outputField)) {
			usedOutputFields.add(outputField);
		} else {
			throw new AggregationException("Output field mapped multiple times: " + outputField);
		}

		// check existence of fields in metadata
		if (!inputField.equals("")) {
			if (inMetadata.getField(inputField) == null) {
				throw new AggregationException("Input field not found: " + inputField);
			}
		}
		if (outMetadata.getField(outputField) == null) {
			throw new AggregationException("Output field not found: " + outputField);
		}
		
		// check if the function is registered
		if (registry.getFunction(functionName) == null) {
			throw new AggregationException("Aggregation function not found: " 
					+ functionName);
		}
		
		checkFunctionFields(functionName, inputField, outputField);
		functionMapping.add(new FunctionMapping(functionName, inputField, outputField));
	}
	
	/**
	 * Check compatibility of fields with an aggregation function.
	 * @param functionName
	 * @param inputField
	 * @param outputField
	 */
	private void checkFunctionFields(String functionName, String inputField, String outputField) 
	throws AggregationException {
		AggregateFunction function;
		try {
			function = registry.getFunction(functionName).newInstance();
		} catch (Exception e) {
			throw new AggregationException("Cannot instantiate aggregation function " + functionName
					+ " : " + e.getMessage(), e);
		}
		
		if (!function.requiresInputField() && !inputField.equals("")) {
			throw new AggregationException("Function " + function.getName() 
					+ " doesn't accept any field as a parameter: " + inputField);
		}
		if (!inputField.equals("")) {
			DataFieldMetadata inputFieldMetadata = inMetadata.getField(inputField);
			try {
				function.checkInputFieldType(inputFieldMetadata);
			} catch (AggregationException e) {
				throw new AggregationException("Input field " + inputField + " has " +
						"invalid type: " + e.getMessage());
			}
		}
		
		try {
			function.checkOutputFieldType(outMetadata.getField(outputField));
		} catch (AggregationException e) {
			throw new AggregationException("Function " + function.getName() 
					+ ": output field " + outputField + " has " +
					"invalid type: " + e.getMessage());
		}
	}
	
	/**
	 * Parses a field mapping.
	 * 
	 * @param expression
	 * @throws AggregationException
	 */
	private void parseFieldMapping(String expression) throws AggregationException {
		String[] parsedExpression = expression.split(Aggregate.ASSIGN_SIGN);
		String inputField = parsedExpression[1].trim().substring(1); // skip the leading "$"
		String outputField = parsedExpression[0].trim();

		// check existence of fields in metadata
		if (!isKeyField(inputField)) {
			throw new AggregationException("Input field is not the key: " + inputField);
		}
		if (usedOutputFields.contains(outputField)) {
			throw new AggregationException("Output field mapped multiple times: " + outputField);
		}
		
		if (inMetadata.getField(inputField) == null) {
			throw new AggregationException("Input field not found: " + inputField);
		}
		if (outMetadata.getField(outputField) == null) {
			throw new AggregationException("Output field not found: " + outputField);
		}
		
		usedOutputFields.add(outputField);
		fieldMapping.add(new FieldMapping(inputField, outputField));
	}
	
	/**
	 * 
	 * @return the field mapping.
	 */
	public List<FieldMapping> getFieldMapping() {
		return fieldMapping;
	}

	/**
	 * 
	 * @return the function mapping.
	 */
	public List<FunctionMapping> getFunctionMapping() {
		return functionMapping;
	}

	/**
	 * 
	 * @param field field name
	 * @return <tt>true</tt> if field is part of the record key.
	 */
	private boolean isKeyField(String field) {
		return keyFields.contains(field);
	}

	/**
	 * Mapping of an aggregation function.
	 * 
	 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
	 *         (c) Javlin Consulting (www.javlinconsulting.cz)
	 *
	 * @created Apr 23, 2007
	 */
	public static class FunctionMapping {
		private String functionName;
		private String inputField;
		private String outputField;
		
		public FunctionMapping(String functionName, String inputField, String outputField) {
			this.functionName = functionName;
			this.inputField = inputField;
			this.outputField = outputField;
		}

		public String getFunctionName() {
			return functionName;
		}

		public String getInputField() {
			return inputField;
		}

		public String getOutputField() {
			return outputField;
		}
	}
	
	/**
	 * Mapping of a field.
	 * 
	 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
	 *         (c) Javlin Consulting (www.javlinconsulting.cz)
	 *
	 * @created Apr 23, 2007
	 */
	public static class FieldMapping {
		private String inputField;
		private String outputField;

		public FieldMapping(String inputField, String outputField) {
			this.inputField = inputField;
			this.outputField = outputField;
		}

		public String getInputField() {
			return inputField;
		}

		public String getOutputField() {
			return outputField;
		}
	}
}
