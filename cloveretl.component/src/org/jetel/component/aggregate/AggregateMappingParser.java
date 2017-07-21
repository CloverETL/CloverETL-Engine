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

import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.string.StringUtils;

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
	private static final String MAPPING_LEFT_SIDE_REGEX = "^\\$[\\w]*\\s*" + Defaults.ASSIGN_SIGN + "\\s*";
	// regexp  matching a correct aggregate function mapping
	private static final String MAPPING_FUNCTION_REGEX = "[\\w ]*\\([\\$\\w ]*\\)";
	// regexp  matching a correct field mapping
	private static final String MAPPING_STRING_REGEX = "\\\".*\\\"";
	// regexp  matching a correct integer constant mapping
	private static final String MAPPING_INT_REGEX = "[\\d]*";
	// regexp  matching a correct double constant mapping
	private static final String MAPPING_DOUBLE_REGEX = "[\\d]*\\.[\\d]*";
	// regexp  matching a correct date constant mapping
	// @see Defaults#DEFAULT_DATE_FORMAT
	private static final String MAPPING_DATE_REGEX = "[\\d]{4}-[\\d]{2}-[\\d]{2}";
	// regexp  matching a correct datetime constant mapping
	// @see Defaults#DEFAULT_DATETIME_FORMAT
	private static final String MAPPING_DATETIME_REGEX = "[\\d]{4}-[\\d]{2}-[\\d]{2} [\\d]{2}:[\\d]{2}:[\\d]{2}";
	// regexp  matching a mapping with a graph parameter
	private static final String MAPPING_PARAM_REGEX = "[\\w ]*\\$\\{[\\w ]*\\}[\\w ]*";
	//matcher for end of line in Pattern.class
	private static final String END_LINE_REGEX = "$";

	private static final Pattern functionPattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_FUNCTION_REGEX + END_LINE_REGEX);
	private static final Pattern fieldPattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + Defaults.CLOVER_FIELD_REGEX + END_LINE_REGEX);
	private static final Pattern stringPattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_STRING_REGEX + END_LINE_REGEX);
	private static final Pattern intPattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_INT_REGEX + END_LINE_REGEX);
	private static final Pattern doublePattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_DOUBLE_REGEX + END_LINE_REGEX);
	private static final Pattern datePattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_DATE_REGEX + END_LINE_REGEX);
	private static final Pattern datetimePattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_DATETIME_REGEX + END_LINE_REGEX);
	private static final Pattern paramPattern = 
		Pattern.compile(MAPPING_LEFT_SIDE_REGEX + MAPPING_PARAM_REGEX + END_LINE_REGEX);
	
	// set of already used output fields, used to detect multiple uses of the same output field
	private Set<String> usedOutputFields = new HashSet<String>(); 
	// fields of the aggregation key
	private Set<String> keyFields;
	// aggregation function registry
	private FunctionRegistry registry;

	private DataRecordMetadata inMetadata;
	private DataRecordMetadata outMetadata;
	
	private boolean paramsAllowed = false;
	private boolean lenient = false;
	
	// error messages generated in the lenient mode
	private List<String> errors = new ArrayList<String>();
	
	private List<FunctionMapping> functionMapping = new ArrayList<FunctionMapping>();
	private List<FieldMapping> fieldMapping = new ArrayList<FieldMapping>();
	private List<ConstantMapping> constantMapping = new ArrayList<ConstantMapping>();
	
	private final DateFormat DATE_FORMAT = new SimpleDateFormat(Defaults.DEFAULT_DATE_FORMAT);
	private final DateFormat DATETIME_FORMAT = new SimpleDateFormat(Defaults.DEFAULT_DATETIME_FORMAT);
	
	/**
	 * 
	 * Allocates a new <tt>AggregateMappingParser</tt> object.
	 *
	 * @param mapping aggregation mapping.
	 * @param paramsAllowed specifies whether graph parameters can be used in the mapping.
	 * @param lenient specifies whether some errors should be treated leniently, i.e. they are
	 * logged (so the error message is available later) and the invalid mapping is skipped.
	 * @param recordKey aggregation key.
	 * @param registry aggregation function registry.
	 * @param inMetadata input metadata.
	 * @param outMetadata output metadata.
	 * @throws AggregationException
	 */
	public AggregateMappingParser(String mapping, boolean paramsAllowed, boolean lenient,
			RecordKey recordKey, FunctionRegistry registry, 
			DataRecordMetadata inMetadata, DataRecordMetadata outMetadata) 
	throws AggregationException {
		this.inMetadata = inMetadata;
		this.outMetadata = outMetadata;
		this.registry = registry;
		this.paramsAllowed = paramsAllowed;
		this.lenient = lenient;
		
		keyFields = new HashSet<String>();
		recordKey.init();
		int[] keyFieldIndices = recordKey.getKeyFields();
		for (int i : keyFieldIndices) {
			keyFields.add(inMetadata.getField(i).getName());
		}

		parseMapping(StringUtils.split(mapping));
	}
	
	/**
	 * @return error messages generated in the <b>lenient</b> mode.
	 */
	public List<String> getErrors() {
		return errors;
	}

	/**
	 * Checks if a text has a valid format for a mapping constant.
	 * @param text
	 * @return <code>true</code> if the text has a valid format for a mapping constant,
	 * <code>false</code> otherwise.
	 */
	public static boolean isValidConstant(String text) {
		String value = text.trim();
		
		// not constants
		if (StringUtils.isBlank(value)) {
			return false;
		}
		if (Pattern.compile("^" + MAPPING_FUNCTION_REGEX + "$").matcher(value).matches()) {
			return false;
		}
		if (Pattern.compile("^" + Defaults.CLOVER_FIELD_REGEX + "$").matcher(value).matches()) {
			return false;
		}
		
		// constants
		if (Pattern.compile("^" + MAPPING_STRING_REGEX + "$").matcher(value).matches()) {
			return true;
		}
		if (Pattern.compile("^" + MAPPING_INT_REGEX + "$").matcher(value).matches()) {
			return true;
		}
		if (Pattern.compile("^" + MAPPING_DATE_REGEX + "$").matcher(value).matches()) {
			return true;
		}
		if (Pattern.compile("^" + MAPPING_DATETIME_REGEX + "$").matcher(value).matches()) {
			return true;
		}
		if (Pattern.compile("^" + MAPPING_DOUBLE_REGEX + "$").matcher(value).matches()) {
			return true;
		}
		if (Pattern.compile("^" + MAPPING_PARAM_REGEX + "$").matcher(value).matches()) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Creates the value of a constant.
	 * 
	 * @param text text of the constant.
	 * @return value of the constant.
	 * @throws AggregationException if the constant format is invalid.
	 */
	public Object getConstantValue(String text) throws AggregationException {
		String value = text.trim();

		if (Pattern.compile("^" + MAPPING_STRING_REGEX + "$").matcher(value).matches()) {
			return createString(value);
		}
		if (Pattern.compile("^" + MAPPING_INT_REGEX + "$").matcher(value).matches()) {
			return createInt(value);
		}
		if (Pattern.compile("^" + MAPPING_DATE_REGEX + "$").matcher(value).matches()) {
			return createDate(value);
		}
		if (Pattern.compile("^" + MAPPING_DATETIME_REGEX + "$").matcher(value).matches()) {
			return createDatetime(value);
		}
		if (Pattern.compile("^" + MAPPING_DOUBLE_REGEX + "$").matcher(value).matches()) {
			return createDouble(value);
		}
		if (Pattern.compile("^" + MAPPING_PARAM_REGEX + "$").matcher(value).matches()) {
			return createString(value);
		}
		
		throw new AggregationException("Invalid mapping format");
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
			Matcher datetimeMatcher = datetimePattern.matcher(expr2);
			Matcher doubleMatcher = doublePattern.matcher(expr2);
			Matcher paramMatcher = paramPattern.matcher(expr2);

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
				} else if (datetimeMatcher.matches()) {
					parseDateTimeConstantMapping(expr2);
				} else if (doubleMatcher.matches()) {
					parseDoubleConstantMapping(expr2);
				} else if (paramsAllowed && paramMatcher.matches()) {
					parseParamMapping(expr2);
				} else {
					throw new AggregationException("Invalid mapping format");
				}
			} catch (NotAKeyFieldException e) {
				String messagePart1 = "Field \"" + e.getInputField() + "\" is not a key field. Aggregation function " 
					+ "must be used ";
				String messagePart2 = "with this field or the field needs to be added to Aggregate key.";
				if (lenient) {
					errors.add(messagePart1 + "\n" + messagePart2);
				} else {
					throw new AggregationException(messagePart1 + messagePart2, e);
				}
			} catch (AggregationException e) {
				String message = "Invalid mapping '" + expr2 + "'";
				if (lenient) {
					errors.add(ExceptionUtils.getMessage(message, e));
				} else {
					throw new AggregationException(message, e);
				}
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
		String[] parsedExpression = expression.split(Defaults.ASSIGN_SIGN);
		String function = parsedExpression[1].trim().replaceAll(" ", "");	// removes additional spaces
		String outputField = parseOutputField(parsedExpression[0]);
		// parse the aggregate function name
		int parenthesesIndex = function.indexOf("(");
		String functionName = function.substring(0, parenthesesIndex).trim().toLowerCase();

		// parse the input field name
		String inputField = null;
		if (parenthesesIndex + 1 == function.indexOf(")")) {
			// right after the left parenthesis is the right parenthesis, so no input field is set
		} else {
			inputField = function.substring(parenthesesIndex + 2, function.length() - 1).trim();
		}

		if (inputField == null) {
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
			throw new AggregationException("Cannot instantiate aggregation function " + functionName + "()", e);
		}
		
		if (!function.requiresInputField() && (inputField != null)) {
			throw new AggregationException("Function " + function.getName() + "()"
					+ " doesn't accept any field as a parameter");
		}
		if (function.requiresInputField() && (inputField == null)) {
			throw new AggregationException("Function " + function.getName() + "()"
					+ " requires an input field as a parameter");
		}
		if (inputField != null) {
			DataFieldMetadata inputFieldMetadata = inMetadata.getField(inputField);
			try {
				function.checkInputFieldType(inputFieldMetadata);
			} catch (AggregationException e) {
				throw new AggregationException("Input field " + inputField + " has invalid type", e);
			}
		}
		
		try {
			function.checkOutputFieldType(outMetadata.getField(outputField));
		} catch (AggregationException e) {
			throw new AggregationException("Function " + function.getName() + "()"
					+ ": output field " + outputField + " has " +
					"invalid type", e);
		}
	}
	
	/**
	 * Parses a field mapping.
	 * 
	 * @param expression
	 * @throws AggregationException
	 * @throws NotAKeyFieldException
	 */
	private void parseFieldMapping(String expression) throws AggregationException, NotAKeyFieldException {
		String[] parsedExpression = expression.split(Defaults.ASSIGN_SIGN);
		String inputField = parsedExpression[1].trim().substring(Defaults.CLOVER_FIELD_INDICATOR.length()); // skip the leading "$"
		String outputField = parseOutputField(parsedExpression[0]);

		checkFieldExistence(inputField, outputField);
		registerOutputFieldUsage(outputField);
		fieldMapping.add(new FieldMapping(inputField, outputField));
		
		// check existence of fields in metadata
		if (!isKeyField(inputField)) {
			throw new NotAKeyFieldException(inputField);
		}
	}
	
	/**
	 * Parses an integer constant mapping.
	 * 
	 * @param expression
	 * @throws AggregationException
	 */
	private void parseIntConstantMapping(String expression) throws AggregationException {
		String[] parsedExpression = expression.split(Defaults.ASSIGN_SIGN);
		String constant = parsedExpression[1].trim();
		String outputField = parseOutputField(parsedExpression[0]);
		
		Integer value = createInt(constant);
		checkFieldExistence(outputField);
		registerOutputFieldUsage(outputField);
		constantMapping.add(ConstantMapping.createIntConstantMapping(value, constant, outputField));
	}

	/**
	 * 
	 * @param constant text of the constant.
	 * @return integer value of the constant.
	 */
	private static Integer createInt(String constant) {
		return Integer.valueOf(constant);
	}
	
	/**
	 * Parses a double constant mapping.
	 * 
	 * @param expression
	 * @throws AggregationException
	 */
	private void parseDoubleConstantMapping(String expression) throws AggregationException {
		String[] parsedExpression = expression.split(Defaults.ASSIGN_SIGN);
		String constant = parsedExpression[1].trim();
		String outputField = parseOutputField(parsedExpression[0]);
		
		Double value = createDouble(constant);
		checkFieldExistence(outputField);
		registerOutputFieldUsage(outputField);
		constantMapping.add(ConstantMapping.createDoubleConstantMapping(value, constant, outputField));
	}
	
	/**
	 * 
	 * @param constant text of the constant.
	 * @return double value of the constant.
	 */
	private static Double createDouble(String constant) {
		return Double.valueOf(constant);
	}

	/**
	 * Parses a string constant mapping.
	 * 
	 * @param expression
	 * @throws AggregationException
	 */
	private void parseStringConstantMapping(String expression) throws AggregationException {
		String[] parsedExpression = expression.split(Defaults.ASSIGN_SIGN);
		// remove the leading and trailing quotation marks
		String constant = parsedExpression[1].trim().substring(1, parsedExpression[1].trim().length() - 1);
		constant = createString(constant); 
		String outputField = parseOutputField(parsedExpression[0]);
		
		checkFieldExistence(outputField);
		registerOutputFieldUsage(outputField);
		constantMapping.add(ConstantMapping.createStringConstantMapping(constant, outputField));
	}

	/**
	 * 
	 * @param constant text of the constant.
	 * @return string value of the constant.
	 */
	private static String createString(String constant) {
		// replace \" with " 
		return constant.replaceAll("\\\\\"", "\""); 
	}
	
	/**
	 * Parses a date constant mapping.
	 * 
	 * @param expression
	 * @throws AggregationException
	 */
	private void parseDateConstantMapping(String expression) throws AggregationException {
		String[] parsedExpression = expression.split(Defaults.ASSIGN_SIGN);
		String constant = parsedExpression[1].trim();
		String outputField = parseOutputField(parsedExpression[0]);
		
		Date value = createDate(constant);
		
		checkFieldExistence(outputField);
		registerOutputFieldUsage(outputField);
		constantMapping.add(ConstantMapping.createDateConstantMapping(value, constant, outputField));
	}

	/**
	 * 
	 * @param constant text of the constant
	 * @return date value of the constant.
	 * @throws AggregationException
	 */
	private Date createDate(String constant) throws AggregationException {
		try {
			return DATE_FORMAT.parse(constant);
		} catch (ParseException e) {
			throw new AggregationException("Date is in invalid  format: " + constant, e);
		}
	}
	
	/**
	 * Parses a datetime constant mapping.
	 * 
	 * @param expression
	 * @throws AggregationException
	 */
	private void parseDateTimeConstantMapping(String expression) throws AggregationException {
		String[] parsedExpression = expression.split(Defaults.ASSIGN_SIGN);
		String constant = parsedExpression[1].trim();
		String outputField = parseOutputField(parsedExpression[0]);
		
		Date value = createDatetime(constant);
		
		checkFieldExistence(outputField);
		registerOutputFieldUsage(outputField);
		constantMapping.add(ConstantMapping.createDateConstantMapping(value, constant, outputField));
	}

	/**
	 * 
	 * @param constant text of the constant
	 * @return datetime value of the constant.
	 * @throws AggregationException
	 */
	private Date createDatetime(String constant) throws AggregationException {
		try {
			return DATETIME_FORMAT.parse(constant);
		} catch (ParseException e) {
			throw new AggregationException("Date is in invalid  format: " + constant, e);
		}
	}

	/**
	 * Parses a mapping containing a graph parameter.
	 * 
	 * @param expression
	 * @throws AggregationException
	 */
	private void parseParamMapping(String expression) throws AggregationException {
		String[] parsedExpression = expression.split(Defaults.ASSIGN_SIGN);
		// remove the leading and trailing quotation marks
		String constant = parsedExpression[1].trim();
		constant = createString(constant); 
		String outputField = parseOutputField(parsedExpression[0]);
		
		checkFieldExistence(outputField);
		registerOutputFieldUsage(outputField);
		constantMapping.add(ConstantMapping.createParamConstantMapping(constant, outputField));
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
	 * Checks whether an output field exists (in the metadata).
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
		
		/**
		 * Allocates a new <tt>FunctionMapping</tt> object.
		 *
		 * @param functionName
		 * @param inputField
		 * @param outputField
		 */
		public FunctionMapping(String functionName, String inputField, String outputField) {
			this.functionName = functionName;
			this.inputField = inputField;
			this.outputField = outputField;
		}

		/**
		 * @return name of the function.
		 */
		public String getFunctionName() {
			return functionName;
		}

		/**
		 * @return name of the input field.
		 */
		public String getInputField() {
			return inputField;
		}

		/**
		 * @return name of the output field.
		 */
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

		/**
		 * Allocates a new <tt>FieldMapping</tt> object.
		 *
		 * @param inputField
		 * @param outputField
		 */
		public FieldMapping(String inputField, String outputField) {
			this.inputField = inputField;
			this.outputField = outputField;
		}

		/**
		 * @return name of the input field.
		 */
		public String getInputField() {
			return inputField;
		}

		/**
		 * @return name of the output field.
		 */
		public String getOutputField() {
			return outputField;
		}
	}
	
	/**
	 * Parses the name of the output field from an aggregation mapping item.
	 * @param expr mapping item.
	 * @return name of the output field.
	 */
	private String parseOutputField(String expr) {
		return expr.trim().substring(Defaults.CLOVER_FIELD_INDICATOR.length());	// skip the leading "$"
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
		 */
		private ConstantMapping() {
			// forces usage of factories
		}
		
		/**
		 * 
		 * Creates a new <tt>ConstantMapping</tt> object.
		 *
		 * @param value integer constant value.
		 * @param stringValue string representation of the value before parsing.
		 * @param outputField name of the output field.
		 * @return integer constant mapping.
		 */
		public static ConstantMapping createIntConstantMapping(int value, String stringValue, String outputField) {
			ConstantMapping result = new ConstantMapping();
			result.value = new Integer(value);
			result.stringValue = stringValue;
			result.outputField = outputField;
			return result;
		}
		
		/**
		 * 
		 * Creates a new <tt>ConstantMapping</tt> object.
		 *
		 * @param value double constant value.
		 * @param stringValue string representation of the value before parsing.
		 * @param outputField name of the output field.
		 * @return double constant mapping.
		 */
		public static ConstantMapping createDoubleConstantMapping(double value, String stringValue, String outputField) {
			ConstantMapping result = new ConstantMapping();
			result.value = new Double(value);
			result.stringValue = stringValue;
			result.outputField = outputField;
			return result;
		}
		
		/**
		 * 
		 * Creates a new <tt>ConstantMapping</tt> object.
		 *
		 * @param value string constant value.
		 * @param outputField name of the output field.
		 * @return string constant mapping.
		 */
		public static ConstantMapping createStringConstantMapping(String value, String outputField) {
			ConstantMapping result = new ConstantMapping();
			result.value = value;
			// the replace adds a backslash in front of '"'
			result.stringValue = "\"" + value.replaceAll("\"", "\\\\\"") + "\"";
			result.outputField = outputField;
			return result;
		}
		
		/**
		 * 
		 * Creates a new <tt>ConstantMapping</tt> object.
		 *
		 * @param value date constant value.
		 * @param stringValue string representation of the value before parsing.
		 * @param outputField name of the output field.
		 * @return date constant mapping.
		 */
		public static ConstantMapping createDateConstantMapping(Date value, String stringValue, String outputField) {
			ConstantMapping result = new ConstantMapping();
			result.value = value;
			result.stringValue = stringValue;
			result.outputField = outputField;
			return result;
		}
		
		/**
		 * 
		 * Creates a new <tt>ConstantMapping</tt> object.
		 *
		 * @param value constant value containing a graph parameter.
		 * @param outputField name of the output field.
		 * @return constant mapping containing a parameter.
		 */
		public static ConstantMapping createParamConstantMapping(String value, String outputField) {
			ConstantMapping result = new ConstantMapping();
			result.value = value;
			result.stringValue = value;
			result.outputField = outputField;
			return result;
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
	
	/**
	 * 
	 * @author Martin Slama (martin.slama@javlin.eu) (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Feb 13th 2012
	 */
	public class NotAKeyFieldException extends Exception {

		private static final long serialVersionUID = 8839038536121748816L;
		private final String inputField;

		/**
		 * Constructor.
		 * @param inputField Field which isn't a key field.
		 */
		public NotAKeyFieldException(String inputField) {
			super();
			this.inputField = inputField;
		}
		
		/**
		 * @return the inputField
		 */
		public String getInputField() {
			return inputField;
		}
	}
}
