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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Applies aggregate functions on records.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
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
	// number of keys in the field mapping
	private int fieldMappingSize;
	
	private boolean sortedGroupChanged = false;
	
	private AggregationGroup sortedGroup;
	private Map<String, AggregationGroup> unsortedGroups;
	private DataRecord previousRecord;
	
	// aggregation key
	private RecordKey recordKey;
	// fields of the aggregation key
	private Set<String> keyFields;
	// true if input is sorted
	private boolean sorted;
	// input metadata
	private DataRecordMetadata inMetadata;
	// output metadata
	private DataRecordMetadata outMetadata;
	// input charset (for CRC32 and MD5)
	private String charset;
	
	/**
	 * 
	 * Allocates a new <tt>AggregateProcessor</tt> object.
	 *
	 * @param mapping aggregate function mapping.
	 * @param oldMapping set to <tt>true</tt> is the old format of the function mapping is used.
	 * @param recordKey aggregation key.
	 * @param sorted set to <tt>true</tt> if the input is sorted.
	 * @param inMetadata metadata of the input.
	 * @param outMetadata metadata of the output.
	 * @param charset charset of the output.
	 * @throws AggregateProcessorException
	 */
	public AggregateProcessor(String[] mapping, boolean oldMapping, RecordKey recordKey, boolean sorted, 
			DataRecordMetadata inMetadata, DataRecordMetadata outMetadata, String charset) 
	throws AggregateProcessorException {
		this.recordKey = recordKey;
		
		keyFields = new HashSet<String>();
		int[] keyFieldIndices = recordKey.getKeyFields();
		for (int i : keyFieldIndices) {
			keyFields.add(inMetadata.getField(i).getName());
		}
		
		this.sorted = sorted;
		if (!sorted) {
			unsortedGroups = new HashMap<String, AggregationGroup>();
		}
		
		this.inMetadata = inMetadata;
		this.outMetadata = outMetadata;
		this.charset = charset;
		initFunctions();
		if (oldMapping) {
			processMapping(convertOldMapping(mapping));
		} else {
			processMapping(mapping);
		}
		
		fieldMappingSize = fieldMapping.keySet().size();
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
		registerFunction(new FirstNonNull());
		registerFunction(new LastNonNull());
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
	 * @throws Exception 
	 */
	public void addRecord(DataRecord inputRecord) throws Exception {
		if (sorted) {
			if (previousRecord == null) {
				// first run
				sortedGroup = new AggregationGroup(inputRecord);
			} 
			if (sortedGroupChanged) {
				// new aggregation group
				sortedGroup.clear(inputRecord);
			}

			sortedGroup.update(inputRecord);
			previousRecord = inputRecord;
		} else {
			String key = recordKey.getKeyString(inputRecord);
			if (!unsortedGroups.containsKey(key)) {
				unsortedGroups.put(key, new AggregationGroup(inputRecord));
			}
			unsortedGroups.get(key).update(inputRecord);
		}
		
		sortedGroupChanged = false;
	}
	
	/**
	 * Returns the current result of aggregation of sorted data. Should be called only when 
	 * the aggregation group has changed.
	 * 
	 * @param outRecord record for storing the output.
	 */
	public void getCurrentSortedAggregationOutput(DataRecord outRecord) {
		sortedGroup.storeResult(outRecord);
		sortedGroupChanged = true;
	}
	
	/**
	 * Returns the result of aggregation of unsorted data.
	 * 
	 * @param outRecord record for storing the output. 
	 * @return result of aggregation of unsorted data.
	 */
	public Iterator<DataRecord> getUnsortedAggregationOutput(DataRecord outRecord) {
		return new UnsortedResultsIterator(outRecord);
	}
	
	/**
	 * Converts the function mapping from the old format to the new one.
	 * 
	 * @param oldMapping function mapping in the old format.
	 * @return function mapping in the new format.
	 */
	private String[] convertOldMapping(String[] oldMapping) {
		String[] result = new String[oldMapping.length + recordKey.getKeyFields().length];

		// process key mappings
		int[] keyFields = recordKey.getKeyFields();
		for (int i = 0; i < keyFields.length; i++) {
			String keyField = inMetadata.getField(keyFields[i]).getName();
			String outField = outMetadata.getField(i).getName();
			result[i] = outField + "=" + keyField;
		}

		// process function mappings
		
		// output fields with indices lesser than this contain key values
		int tmpIndex = recordKey.getKeyFields().length;
		for (int i = tmpIndex; i < result.length; i++) {
			String outField = outMetadata.getField(i).getName();
			result[i] = outField + "=" + oldMapping[i - tmpIndex];  
		}

		return result;
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

				if (!isKeyField(inputField)) {
					throw new AggregateProcessorException("Input field is not the key: " + inputField);
				}
				if (usedOutputFields.contains(outputField)) {
					throw new AggregateProcessorException("Output field mapped multiple times: " + outputField);
				}
				
				usedOutputFields.add(outputField);
				addFieldMapping(inputField, outputField);
			} else {
				throw new AggregateProcessorException("Invalid aggregate function mapping: "
						+ expression);
			}
		}
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
	 * @throws AggregateProcessorException
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
			try {
				function.checkInputFieldType(inputFieldMetadata);
			} catch (AggregateProcessorException e) {
				throw new AggregateProcessorException("Input field " + inputField + " has " +
						"invalid type: " + e.getMessage());
			}
			function.setInputFieldMetadata(inputFieldMetadata);
		}
		
		if (outMetadata.getField(outputField) == null) {
			throw new AggregateProcessorException("Output field not found: " + outputField);
		}
		try {
			function.checkOutputFieldType(outMetadata.getField(outputField));
		} catch (AggregateProcessorException e) {
			throw new AggregateProcessorException("Function " + function.getName() 
					+ ": output field " + outputField + " has " +
					"invalid type: " + e.getMessage());
		}
	}
	
	/**
	 * Mapping of an aggregation function onto an input and output field.
	 * 
	 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
	 *         (c) Javlin Consulting (www.javlinconsulting.cz)
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
		 * @param function
		 * @param inputField 
		 * @param outputField
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
	
	/**
	 * One aggregation group with its aggregation functions and their intermediate results.
	 * 
	 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
	 *         (c) Javlin Consulting (www.javlinconsulting.cz)
	 */
	private class AggregationGroup {
		private KeyFieldItem[] keyFields;
		private AggregateFunction[] functions;
		
		public AggregationGroup(DataRecord firstInput) throws AggregateProcessorException {
			storeKeyFields(firstInput);
			int count = functionMapping.size();
			functions = new AggregateFunction[count];
			for (int i = 0; i < count; i++) {
				FunctionMappingItem mapping = functionMapping.get(i);
				AggregateFunction function = createFunctionInstance(mapping.getFunction());
				function.setInputFieldIndex(mapping.getInputFieldIndex());
				function.setInputFieldMetadata(inMetadata.getField(mapping.getInputFieldIndex()));
				function.setOutputFieldIndex(mapping.getOutputFieldIndex());
				
				functions[i] = function;
			}
		}
		
		public void update(DataRecord inputRecord) throws Exception {
			for (AggregateFunction function : functions) {
				function.update(inputRecord);
			}
		}
		
		public void storeResult(DataRecord outRecord) {
			outRecord.setToNull();
			applyFieldMapping(outRecord);
			for (AggregateFunction function : functions) {
				function.storeResult(outRecord.getField(function.getOutputFieldIndex()));
			}
		}
		
		public void clear(DataRecord firstInput) {
			storeKeyFields(firstInput);
			for (AggregateFunction function : functions) {
				function.clear();
			}
		}
		
		/**
		 * Stored the DataFields which are the key; but only those used in the field mapping.
		 * @param input
		 */
		private void storeKeyFields(DataRecord input) {
			keyFields = new KeyFieldItem[fieldMappingSize];
			Set<Integer> outputFields = fieldMapping.keySet();
			int i = 0;
			for (Integer outputFieldIndex : outputFields) {
				keyFields[i] = new KeyFieldItem(outputFieldIndex, 
						input.getField(fieldMapping.get(outputFieldIndex)).duplicate());
				i++;
			}
		}
		
		/**
		 * Applies the field mapping, i.e. copies the values of keys.
		 * 
		 * @param outputRecord
		 */
		private void applyFieldMapping(DataRecord outputRecord) {
			for (KeyFieldItem item : keyFields) {
				outputRecord.getField(item.getIndex()).setValue(item.getField());
			}
		}

		/**
		 * Stores the values of keys which have to be copied to the result later.
		 * 
		 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
		 *         (c) Javlin Consulting (www.javlinconsulting.cz)
		 */
		private class KeyFieldItem {
			// index of output field where the key will be stored
			int index;
			// key value from input
			DataField field;
			
			/**
			 * 
			 * Allocates a new <tt>KeyFieldItem</tt> object.
			 *
			 * @param index index of output field where the key will be stored.
			 * @param field key value
			 */
			public KeyFieldItem(int index, DataField field) {
				this.index = index;
				this.field = field;
			}

			/**
			 * @return the field
			 */
			public DataField getField() {
				return field;
			}

			/**
			 * @return the index
			 */
			public int getIndex() {
				return index;
			}
		}
	}
	
	/**
	 * Iterator over the results of unsorted aggregation.
	 * 
	 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
	 *         (c) Javlin Consulting (www.javlinconsulting.cz)
	 */
	private class UnsortedResultsIterator implements Iterator<DataRecord> {
		Iterator<String> keyIterator;
		DataRecord outRecord;
		
		public UnsortedResultsIterator(DataRecord outRecord) {
			this.outRecord = outRecord;
			keyIterator = unsortedGroups.keySet().iterator();
		}
		public boolean hasNext() {
			return keyIterator.hasNext();
		}

		public DataRecord next() {
			unsortedGroups.get(keyIterator.next()).storeResult(outRecord);
			return outRecord;
		}

		public void remove() {
			throw new UnsupportedOperationException("Removal of aggregation results is not supported");
		}
	}
}
