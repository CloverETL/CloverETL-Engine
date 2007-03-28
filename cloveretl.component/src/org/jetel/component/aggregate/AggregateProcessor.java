package org.jetel.component.aggregate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Applies aggregate functions on records.
 * 
 * @author Jaroslav Urban
 *
 */
public class AggregateProcessor {
	// regular expression matching a correct aggregate function mapping
	private static final String MAPPING_FUNCTION_REGEX = "^[\\w ]*=[\\w ]*\\([\\w ]*\\)$";
	// regular expression matching a correct field mapping
	private static final String MAPPING_FIELD_REGEX = "^[\\w ]*=[\\w ]*$";
	
	// registry of available aggregate functions, key is the function name (lowercase)
	private Map<String, Class<? extends AggregateFunction>> functions = 
		new HashMap<String, Class<? extends AggregateFunction>>();
	// function mapping
	private List<FunctionMappingItem> functionMapping = new ArrayList<FunctionMappingItem>();
	// field mapping, key is the output field, value is the input field
	private Map<Integer, Integer> fieldMapping = new HashMap<Integer, Integer>();
	
	private AggregationGroup sortedGroup;
	private Map<String, AggregationGroup> unsortedGroups;
	private DataRecord previousRecord;
	
	private RecordKey recordKey;
	private boolean sorted;
	private DataRecordMetadata inMetadata;
	private DataRecordMetadata outMetadata;
	private String charset;
	
	/**
	 * 
	 * Allocates a new <tt>AggregateProcessor</tt> object.
	 *
	 * @param mapping aggregate function mapping.
	 * @param recordKey aggregation key.
	 * @param sorted set to <tt>true</tt> if the input is sorted.
	 * @param inMetadata metadata of the input.
	 * @param outMetadata metadata of the output.
	 * @param charset charset of the output.
	 * @throws ProcessorInitializationException
	 */
	public AggregateProcessor(String[] mapping, RecordKey recordKey, boolean sorted, 
			DataRecordMetadata inMetadata, DataRecordMetadata outMetadata, String charset) 
	throws AggregateProcessorException {
		this.recordKey = recordKey;
		this.sorted = sorted;
		if (!sorted) {
			unsortedGroups = new HashMap<String, AggregationGroup>();
		}
		
		this.inMetadata = inMetadata;
		this.outMetadata = outMetadata;
		this.charset = charset;
		initFunctions();
		processMapping(mapping);
	}

	/**
	 * Initializes aggregation funcions. All aggregation functions must be initialized and 
	 * registered here.
	 * 
	 * @throws ProcessorInitializationException
	 */
	private void initFunctions() throws AggregateProcessorException {
		registerFunction(new Count());
		registerFunction(new Min());
		registerFunction(new Max());
		registerFunction(new Sum());
		registerFunction(new First());
		registerFunction(new Last());
		registerFunction(new Avg());
		registerFunction(new StdDev());
		registerFunction(new CRC32());
		registerFunction(new MD5());
	}
	
	/**
	 * Registers an aggregation function, i.e. makes it available for use during aggregation.
	 * @param f
	 */
	private void registerFunction(AggregateFunction f) {
		if (getFunction(f.getName()) != null) {
			throw new IllegalArgumentException("Aggregate function already registered: " + f.getName());
		}
		addFunction(f.getName(), f.getClass());
	}
	
	/**
	 * Adds an aggregation function to the registry.
	 * 
	 * @param name name
	 * @param f
	 */
	private void addFunction(String name, Class<? extends AggregateFunction> f) {
		functions.put(name.toLowerCase(), f);
	}
	
	/**
	 * 
	 * @param name name of the aggregation function.
	 * @return aggregation function from the registry.
	 */
	private Class<? extends AggregateFunction> getFunction(String name) {
		return functions.get(name.toLowerCase());
	}
	
	/**
	 * Processes a record from the input.
	 * 
	 * @param inputRecord record from the input.
	 * @param outputRecord record for storing the output.
	 */
	public void addRecord(DataRecord inputRecord, DataRecord outputRecord) throws Exception {
		// apply the field mapping
		Set<Integer> outputFields = fieldMapping.keySet();
		for (Integer outputField : outputFields) {
			outputRecord.getField(outputField).copyFrom(inputRecord.getField(fieldMapping.get(outputField)));
		}
		
		
		if (sorted) {
			if ((previousRecord == null)
					|| !recordKey.equals(previousRecord, inputRecord)) {
				// if first run or the aggregation group has changed

				sortedGroup = new AggregationGroup();
			} 

			sortedGroup.update(inputRecord, outputRecord);
			previousRecord = inputRecord;
		} else {
			String key = recordKey.getKeyString(inputRecord);
			if (!unsortedGroups.containsKey(key)) {
				unsortedGroups.put(key, new AggregationGroup());
			}
			unsortedGroups.get(key).update(inputRecord, outputRecord);
		}
	}
	
	/**
	 * Returns the current result of aggregation of sorted data. Should be called when 
	 * the aggregation group has changed.
	 * 
	 * @param outRecord record used in {@link #addRecord(DataRecord, DataRecord)} for storing the output.
	 * @return current result of aggregation.
	 */
	public DataRecord getCurrentSortedAggregationOutput(DataRecord outRecord) {
		DataRecord result = outRecord.duplicate();
		sortedGroup.storeResult(result);
		return result;
	}
	
	/**
	 * Returns the result of aggregation of unsorted data.
	 * 
	 * @return result of aggregation of unsorted data.
	 */
	public Iterator<DataRecord> getUnsortedAggregationOutput() {
		return new UnsortedResultsIterator();
	}
	
	/**
	 * Processes the aggregation function mapping.
	 * 
	 * @param mapping the function mapping.
	 * @throws ProcessorInitializationException
	 */
	private void processMapping(String[] mapping) throws AggregateProcessorException {
		Pattern functionPattern = Pattern.compile(MAPPING_FUNCTION_REGEX);
		Pattern fieldPattern = Pattern.compile(MAPPING_FIELD_REGEX);
		// set of already used output fields, used to detect multiple uses of the same output field
		Set<String> usedOutputFields = new HashSet<String>(); 
		
		for (String expression : mapping) {
			Matcher functionMatcher = functionPattern.matcher(expression);
			Matcher fieldMatcher = fieldPattern.matcher(expression);
			
			if (functionMatcher.matches()) {
				// mapping contains an aggregate function
				
				// parse the output field name
				int equalsIndex = expression.indexOf("=");
				String outputField = expression.substring(0, equalsIndex).trim();

				// parse the aggregate function name
				int parenthesesIndex = expression.indexOf("(", equalsIndex + 1);
				String functionName = expression.substring(equalsIndex + 1, parenthesesIndex).trim();

				// parse the input field name
				String inputField = expression.substring(parenthesesIndex + 1, expression.length() - 1).trim();

				if (!usedOutputFields.contains(outputField)) {
					usedOutputFields.add(outputField);
				} else {
					throw new AggregateProcessorException("Output field mapped multiple times: " + outputField);
				}
				
				addFunctionMapping(functionName, inputField, outputField);
			} else if (fieldMatcher.matches()) {
				// mapping contains the name of a field in input
				
				String[] parsedExpression = expression.split("=");
				String inputField = parsedExpression[1].trim();
				String outputField = parsedExpression[0].trim();

				if (!usedOutputFields.contains(outputField)) {
					usedOutputFields.add(outputField);
				} else {
					throw new AggregateProcessorException("Output field mapped multiple times: " + outputField);
				}

				addFieldMapping(inputField, outputField);
			} else {
				throw new AggregateProcessorException("Invalid aggregate function mapping: "
						+ expression);
			}
		}
	}
	
	/**
	 * Adds a mapping of an aggregation function.
	 * 
	 * @param functionName name of the aggregation function.
	 * @param inputField input field used as a parameter of the aggregation function.
	 * @param outputField output field for the result of the aggregation function.
	 * @throws ProcessorInitializationException
	 */
	private void addFunctionMapping(String functionName, String inputField, String outputField)
	throws AggregateProcessorException {
		AggregateFunction f = createFunctionInstance(functionName);
		checkFields(f, inputField, outputField);
		
		functionMapping.add(new FunctionMappingItem(functionName, inputField, outputField));
	}

	/**
	 * Creates an instance of an aggregation function.
	 * 
	 * @param name name of the aggregation function.
	 * @return aggregation function.
	 * @throws ProcessorInitializationException
	 */
	private AggregateFunction createFunctionInstance(String name) throws AggregateProcessorException {
		AggregateFunction result = null;
		if (getFunction(name) == null) {
			throw new AggregateProcessorException("Aggregate function not found: "
					+ name);
		}
		
		try {
			result = getFunction(name).newInstance();
			result.setRecordKey(recordKey);
			result.setSorted(sorted);
			result.setCharset(charset);
			result.init();
		} catch (InstantiationException e) {
			throw new AggregateProcessorException("Cannot instantiate aggregate function", e);
		} catch (IllegalAccessException e) {
			throw new AggregateProcessorException("Cannot instantiate aggregate function", e);
		} 
		
		return result; 
	}

	/**
	 * Checks compatibility of input and output fields with an aggregation function. The data types
	 * of the fields and the presence of an input field (as a parameter for a function) are checked.
	 *  
	 * @param function
	 * @param inputField
	 * @param outputField
	 * @throws ProcessorInitializationException
	 */
	private void checkFields(AggregateFunction function, String inputField, String outputField) 
	throws AggregateProcessorException {
		if (!function.requiresInputField() && !inputField.equals("")) {
			throw new AggregateProcessorException("Function " + function.getName() 
					+ " doesn't accept any field as a parameter: " + inputField);
		}
		if (!inputField.equals("")) {
			DataFieldMetadata inputFieldMetadata = inMetadata.getField(inputField);
			if (inputFieldMetadata == null) {
				throw new AggregateProcessorException("Input field not found: " + inputField);
			}
			function.checkInputFieldType(inputFieldMetadata);
			function.setInputFieldName(inputField);
			function.setInputFieldMetadata(inputFieldMetadata);
		}
		
		if (outMetadata.getField(outputField) == null) {
			throw new AggregateProcessorException("Output field not found: " + outputField);
		}
		function.checkOutputFieldType(outMetadata.getField(outputField));
	}
	
	/**
	 * Mapping of an aggregation function onto an input and output field.
	 * 
	 * @author Jaroslav Urban
	 *
	 */
	private class FunctionMappingItem {
		private String inputField;
		private int inputFieldIndex;
		private String outputField;
		private int outputFieldIndex;
		private String function;
		
		/**
		 * 
		 * Allocates a new <tt>FunctionMappingItem</tt> object.
		 *
		 * @param outputField
		 * @param function
		 */
		public FunctionMappingItem(String function, String inputField, String outputField) {
			this.inputField = inputField;
			this.inputFieldIndex = inMetadata.getFieldPosition(inputField);
			this.outputField = outputField;
			this.outputFieldIndex = outMetadata.getFieldPosition(outputField);
			this.function = function;
		}

		/**
		 * @return the function
		 */
		public String getFunction() {
			return function;
		}

		/**
		 * @return the input field
		 */
		public String getInputField() {
			return inputField;
		}

		/**
		 * @return the output field
		 */
		public String getOutputField() {
			return outputField;
		}

		/**
		 * @return the input field index
		 */
		public int getInputFieldIndex() {
			return inputFieldIndex;
		}

		/**
		 * @return the output field index
		 */
		public int getOutputFieldIndex() {
			return outputFieldIndex;
		}
	}
	
	/**
	 * Registers a mapping between input and output fields.  Used to store the values of 
	 * aggregation keys in the output.
	 * 
	 * @param inputField
	 * @param outputField
	 * @throws ProcessorInitializationException
	 */
	private void addFieldMapping(String inputField, String outputField)
	throws AggregateProcessorException {
		if (inMetadata.getField(inputField) == null) {
			throw new AggregateProcessorException("Input field not found: " + inputField);
		}
		if (outMetadata.getField(outputField) == null) {
			throw new AggregateProcessorException("Output field not found: " + outputField);
		}
		
		fieldMapping.put(outMetadata.getFieldPosition(outputField), 
				inMetadata.getFieldPosition(inputField));
	}
	
	private class AggregationGroup {
		private AggregateFunction[] functions;
		DataRecord outputRecord;
		
		public AggregationGroup() throws AggregateProcessorException {
			int count = functionMapping.size();
			functions = new AggregateFunction[count];
			for (int i = 0; i < count; i++) {
				FunctionMappingItem mapping = functionMapping.get(i);
				AggregateFunction function = createFunctionInstance(mapping.getFunction());
				function.setInputFieldName(mapping.getInputField());
				function.setInputFieldIndex(mapping.getInputFieldIndex());
				function.setInputFieldMetadata(inMetadata.getField(mapping.getInputFieldIndex()));
				function.setOutputFieldName(mapping.getOutputField());
				function.setOutputFieldIndex(mapping.getOutputFieldIndex());
				
				functions[i] = function;
			}
		}
		
		public void update(DataRecord inputRecord, DataRecord outputRecord) throws Exception {
			this.outputRecord = outputRecord.duplicate();
			
			for (AggregateFunction function : functions) {
				function.update(inputRecord);
			}
		}
		
		public void storeResult(DataRecord outRecord) {
			outRecord.copyFrom(outputRecord);	// copies the applied field mappings
			for (AggregateFunction function : functions) {
				function.storeResult(outRecord.getField(function.getOutputFieldIndex()));
			}
		}
	}
	
	/**
	 * Iterator over the results of unsorted aggregation.
	 * 
	 * @author Jaroslav Urban
	 *
	 */
	private class UnsortedResultsIterator implements Iterator<DataRecord> {
		Iterator<String> keyIterator;
		
		public UnsortedResultsIterator() {
			keyIterator = unsortedGroups.keySet().iterator();
		}
		public boolean hasNext() {
			return keyIterator.hasNext();
		}

		public DataRecord next() {
			DataRecord outRecord = new DataRecord(outMetadata);
			outRecord.init();
			unsortedGroups.get(keyIterator.next()).storeResult(outRecord);
			return outRecord;
		}

		public void remove() {
			throw new UnsupportedOperationException("Removal of aggregation results is not supported");
		}
	}
}
