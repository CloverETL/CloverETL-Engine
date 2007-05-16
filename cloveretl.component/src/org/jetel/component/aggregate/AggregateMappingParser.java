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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.component.Aggregate;
import org.jetel.data.Defaults;
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
	// regexp matching the left half of an aggregation mapping item (including the assign sign)
	private static final String MAPPING_LEFT_SIDE_REGEX = "^[\\w]*[ ]*" + Aggregate.ASSIGN_SIGN + "[ ]*";
	// regular expression matching a correct aggregate function mapping
	private static final String MAPPING_FUNCTION_REGEX = "[\\w ]*\\([\\w ]*\\)";
	// regular expression matching a correct field mapping
	private static final String MAPPING_FIELD_REGEX = "\\$[\\w ]*";
	// regular expression matching a correct string constant mapping
	private static final String MAPPING_STRING_REGEX = "\\\"[\\w .,!?'-]*\\\"";
	// regular expression matching a correct integer constant mapping
	private static final String MAPPING_INT_REGEX = "[\\d]*";
	// regular expression matching a correct date constant mapping
	private static final String MAPPING_DATE_REGEX = "[\\d]{4}-[\\d]{2}-[\\d]{2}";

	private static final Pattern functionPattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_FUNCTION_REGEX + "$");
	private static final Pattern fieldPattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_FIELD_REGEX + "$");
	private static final Pattern stringPattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_STRING_REGEX + "$");
	private static final Pattern intPattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_INT_REGEX + "$");
	private static final Pattern datePattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_DATE_REGEX + "$");
	
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
	private List<ConstantMapping> constantMapping = new ArrayList<ConstantMapping>();
	
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat(Defaults.DEFAULT_DATE_FORMAT);
	
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
			if (expr2.equals("")) {
				continue;
			}
			Matcher functionMatcher = functionPattern.matcher(expr2);
			Matcher fieldMatcher = fieldPattern.matcher(expr2);
			Matcher stringMatcher = stringPattern.matcher(expr2);
			Matcher intMatcher = intPattern.matcher(expr2);
			Matcher dateMatcher = datePattern.matcher(expr2);

			try {
				if (functionMatcher.matches()) {
					parseFunction(expr2);
				} else if (fieldMatcher.matches()) {
					parseFieldMapping(expr2);
				} else if (stringMatcher.matches()) {
					parseStringConstantMapping(expr2);
				} else if (intMatcher.matches()) {
					parseIntConstantMapping(expr2);
				} else if (dateMatcher.matches()) {
					parseDateConstantMapping(expr2);
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

		if (inputField.equals("")) {
			checkFieldExistence(outputField);
		} else {
			checkFieldExistence(inputField, outputField);
		}
		
		// check if the function is registered
		if (registry.getFunction(functionName) == null) {
			throw new AggregationException("Aggregation function not found: " 
					+ functionName);
		}
		
		checkFunctionFields(functionName, inputField, outputField);
		registerOutputFieldUsage(outputField);
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
		if (function.requiresInputField() && inputField.equals("")) {
			throw new AggregationException("Function " + function.getName()
					+ " requires an input field as a parameter");
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

		checkFieldExistence(inputField, outputField);
		registerOutputFieldUsage(outputField);
		fieldMapping.add(new FieldMapping(inputField, outputField));
	}
	
	/**
	 * Parses an integer constant mapping.
	 * 
	 * @param expression
	 * @throws AggregationException
	 */
	private void parseIntConstantMapping(String expression) throws AggregationException {
		String[] parsedExpression = expression.split(Aggregate.ASSIGN_SIGN);
		String constant = parsedExpression[1].trim();
		String outputField = parsedExpression[0].trim();
		
		Integer value = Integer.valueOf(constant);
		checkFieldExistence(outputField);
		registerOutputFieldUsage(outputField);
		constantMapping.add(new ConstantMapping(value, constant, outputField));
	}

	/**
	 * Parses a string constant mapping.
	 * 
	 * @param expression
	 * @throws AggregationException
	 */
	private void parseStringConstantMapping(String expression) throws AggregationException {
		String[] parsedExpression = expression.split(Aggregate.ASSIGN_SIGN);
		// remove the leading and trailing quotation marks
		String constant = parsedExpression[1].trim().replaceAll("\"", "");	
		String outputField = parsedExpression[0].trim();
		
		checkFieldExistence(outputField);
		registerOutputFieldUsage(outputField);
		constantMapping.add(new ConstantMapping(constant, outputField));
	}

	/**
	 * Parses a date constant mapping.
	 * 
	 * @param expression
	 * @throws AggregationException
	 */
	private void parseDateConstantMapping(String expression) throws AggregationException {
		String[] parsedExpression = expression.split(Aggregate.ASSIGN_SIGN);
		String constant = parsedExpression[1].trim();
		String outputField = parsedExpression[0].trim();
		
		Date value;
		try {
			value = DATE_FORMAT.parse(constant);
		} catch (ParseException e) {
			throw new AggregationException("Date is in invalid  format: " + constant, e);
		}
		checkFieldExistence(outputField);
		registerOutputFieldUsage(outputField);
		constantMapping.add(new ConstantMapping(value, constant, outputField));
	}

	/**
	 * Registers an output field as used (by some mapping).
	 * 
	 * @param outputField name of the output field.
	 * @throws AggregationException if the output field is already used.
	 */
	private void registerOutputFieldUsage(String outputField) throws AggregationException {
		if (!usedOutputFields.contains(outputField)) {
			usedOutputFields.add(outputField);
		} else {
			throw new AggregationException("Output field mapped multiple times: " + outputField);
		}
	}
	
	/**
	 * Checks whether the fields exist (in the metadata).
	 * 
	 * @param inputField name of an input field.
	 * @param outputField name of an output field.
	 * @throws AggregationException if any of the fields doesn't exist.
	 */
	private void checkFieldExistence(String inputField, String outputField) throws AggregationException {
		if (inMetadata.getField(inputField) == null) {
			throw new AggregationException("Input field not found: " + inputField);
		}
		checkFieldExistence(outputField);
	}

	/**
	 * Checkss whether an output field exists (in the metadata).
	 * 
	 * @param outputField name of an output field.
	 * @throws AggregationException if the output field doesn't exist.
	 */
	private void checkFieldExistence(String outputField) throws AggregationException {
		if (outMetadata.getField(outputField) == null) {
			throw new AggregationException("Output field not found: " + outputField);
		}
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
	 * @return the constant mapping.
	 */
	public List<ConstantMapping> getConstantMapping() {
		return constantMapping;
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
	
	/**
	 * Mapping of a constant.
	 * 
	 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
	 *         (c) Javlin Consulting (www.javlinconsulting.cz)
	 *
	 * @created 30.4.2007
	 */
	public static class ConstantMapping {
		// name of the output field
		private String outputField;
		// value of the constant
		private Object value;
		// value of the constant before parsing 
		private String stringValue;
		
		/**
		 * 
		 * Allocates a new <tt>ConstantMapping</tt> object.
		 *
		 * @param value integer constant value.
		 * @param stringValue string representation of the value before parsing.
		 * @param outputField name of the output field.
		 */
		public ConstantMapping(int value, String stringValue, String outputField) {
			this.value = new Integer(value);
			this.stringValue = stringValue;
			this.outputField = outputField;
		}

		/**
		 * 
		 * Allocates a new <tt>ConstantMapping</tt> object.
		 *
		 * @param value string constant value.
		 * @param outputField name of the output field.
		 */
		public ConstantMapping(String value, String outputField) {
			this.value = value;
			this.stringValue = "\"" + value + "\"";
			this.outputField = outputField;
		}
		
		/**
		 * 
		 * Allocates a new <tt>ConstantMapping</tt> object.
		 *
		 * @param value
		 * @param stringValue string representation of the value before parsing.
		 * @param outputField name of the output field.
		 */
		public ConstantMapping(Date value, String stringValue, String outputField) {
			this.value = value;
			this.stringValue = stringValue;
			this.outputField = outputField;
		}

		/**
		 * 
		 * @return name of the output field where the constant will be stored.
		 */
		public String getOutputField() {
			return outputField;
		}

		/**
		 * 
		 * @return value of the constant.
		 */
		public Object getValue() {
			return value;
		}

		/**
		 * 
		 * @return string representation of the constant before parsing.
		 */
		public String getStringValue() {
			return stringValue;
		}
	}
}
