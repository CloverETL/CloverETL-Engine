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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Applies aggregate functions on records.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class AggregateProcessor {
	// registry of available aggregate functions
	private FunctionRegistry functionRegistry = new FunctionRegistry();
	// function mapping
	private List<FunctionMappingItem> functionMapping = new ArrayList<FunctionMappingItem>();
	// field mapping, key is the output field, value is the input field
	private Map<Integer, Integer> fieldMapping = new HashMap<Integer, Integer>();
	// number of keys in the field mapping
	private int fieldMappingSize;
	// constants mapping
	private List<ConstantMappingItem> constantMapping = new ArrayList<ConstantMappingItem>();
	
	// indicates that the aggregator just detected the start of a new aggregation group in sorted input
	private boolean sortedGroupChanged = false;
	
	private AggregationGroup sortedGroup;
	private HashKey hashKey;
	private Map<HashKey, AggregationGroup> unsortedGroups;
	private DataRecord previousRecord;
	
	// aggregation key
	private RecordKey recordKey;
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
	 * @throws AggregationException
	 */
	public AggregateProcessor(String mapping, boolean oldMapping, RecordKey recordKey, boolean sorted, 
			DataRecordMetadata inMetadata, DataRecordMetadata outMetadata, String charset) 
	throws AggregationException {
		this.recordKey = recordKey;
		
		this.sorted = sorted;
		if (!sorted) {
			hashKey = new HashKey(recordKey, null);
			unsortedGroups = new HashMap<HashKey, AggregationGroup>();
		}
		
		this.inMetadata = inMetadata;
		this.outMetadata = outMetadata;
		this.charset = charset;
		if (oldMapping) {
			processMapping(convertOldMapping(mapping));
		} else {
			processMapping(mapping);
		}
		
		fieldMappingSize = fieldMapping.keySet().size();
	}

	/**
	 * Reset the processor to initial state.
	 */
	public void reset() {
		previousRecord = null;
		sortedGroupChanged = false;
		sortedGroup = null;
		if (unsortedGroups != null) {
			unsortedGroups.clear();
		}
	}
	
	/**
	 * 
	 * @param name name of the aggregation function.
	 * @return aggregation function from the registry.
	 */
	private Class<? extends AggregateFunction> getFunction(String name) {
		return functionRegistry.getFunction(name);
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
			hashKey.setDataRecord(inputRecord);
			AggregationGroup group = unsortedGroups.get(hashKey);
			if (group == null) {
				DataRecord storedRecord = inputRecord.duplicate();
				AggregationGroup newGroup = new AggregationGroup(storedRecord);
				unsortedGroups.put(new HashKey(recordKey, storedRecord), newGroup);
				newGroup.update(inputRecord);
			} else {
				group.update(inputRecord);
			}
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
	private String convertOldMapping(String oldMapping) {
		String[] oldMappingSplit = oldMapping.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		String[] result = new String[oldMappingSplit.length + recordKey.getKeyFields().length];

		// process key mappings (int the old mapping, all key fields were copied
		// to the first output fields)
		
		int[] keyFields = recordKey.getKeyFields();
		for (int i = 0; i < keyFields.length; i++) {
			String keyField = inMetadata.getField(keyFields[i]).getName();
			String outField = outMetadata.getField(i).getName();
			result[i] = Defaults.CLOVER_FIELD_INDICATOR + outField + Defaults.ASSIGN_SIGN + Defaults.CLOVER_FIELD_INDICATOR
					+ keyField;
		}

		// process function mappings
		
		// output fields with indices lesser than the tmpIndex contain key values, skip them
		int tmpIndex = recordKey.getKeyFields().length;
		for (int i = tmpIndex; i < result.length; i++) {
			String outField = outMetadata.getField(i).getName();
			
			// convert the function mapping from functionName(parameter) to functionName($parameter)
			String oldMappingItem = oldMappingSplit[i - tmpIndex].trim();
			int parenthesesIndex = oldMappingItem.indexOf("(");
			String functionName = oldMappingItem.substring(0, parenthesesIndex).trim().toLowerCase();
			String inputField = null;
			if (parenthesesIndex + 1 == oldMappingItem.indexOf(")")) {
				// right after the left parenthesis is the right parenthesis, so no input field is set
			} else {
				inputField = oldMappingItem.substring(parenthesesIndex + 1, oldMappingItem.length() - 1).trim();
			}
			String newMappingItem = functionName + "(";
			if (inputField != null) {
				newMappingItem += Defaults.CLOVER_FIELD_INDICATOR + inputField;
			}
			newMappingItem += ")";
			
			result[i] = Defaults.CLOVER_FIELD_INDICATOR + outField + Defaults.ASSIGN_SIGN + newMappingItem;  
		}

		String resultString = new String();
		for (String s : result) {
			resultString += s + Defaults.Component.KEY_FIELDS_DELIMITER;
		}
		return resultString;
	}
	/**
	 * Processes the aggregation function mapping.
	 * 
	 * @param mapping the function mapping.
	 * @throws ProcessorInitializationException
	 */
	private void processMapping(String mapping) throws AggregationException {
		AggregateMappingParser parser = new AggregateMappingParser(mapping, false, false,
				recordKey, functionRegistry,
				inMetadata, outMetadata);
		List<AggregateMappingParser.FunctionMapping> functionMappings = parser.getFunctionMapping();
		List<AggregateMappingParser.FieldMapping> fieldMappings = parser.getFieldMapping();
		List<AggregateMappingParser.ConstantMapping> constantMappings = parser.getConstantMapping();
		
		for (AggregateMappingParser.FieldMapping fieldMapping : fieldMappings) {
			addFieldMapping(fieldMapping.getInputField(), fieldMapping.getOutputField());
		}
		for (AggregateMappingParser.FunctionMapping functionMapping : functionMappings) {
			addFunctionMapping(functionMapping.getFunctionName(), 
					functionMapping.getInputField(),
					functionMapping.getOutputField());
		}
		for (AggregateMappingParser.ConstantMapping constantMapping : constantMappings) {
			addConstantMapping(constantMapping.getValue(), constantMapping.getOutputField());
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
	throws AggregationException {
		AggregateFunction f = createFunctionInstance(functionName);

		if (inputField != null) {
			f.setInputFieldMetadata(inMetadata.getField(inputField));
		}
		f.setOutputFieldMetadata(outMetadata.getField(outputField));
		
		functionMapping.add(new FunctionMappingItem(functionName, inputField, outputField));
	}

	/**
	 * Creates an instance of an aggregation function.
	 * 
	 * @param name name of the aggregation function.
	 * @return aggregation function.
	 * @throws ProcessorInitializationException
	 */
	private AggregateFunction createFunctionInstance(String name) throws AggregationException {
		AggregateFunction result = null;
		try {
			result = getFunction(name).newInstance();
			result.setRecordKey(recordKey);
			result.setSorted(sorted);
			result.setCharset(charset);
			result.init();
		} catch (InstantiationException e) {
			throw new AggregationException("Cannot instantiate aggregate function", e);
		} catch (IllegalAccessException e) {
			throw new AggregationException("Cannot instantiate aggregate function", e);
		} 
		
		return result; 
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
	throws AggregationException {
		fieldMapping.put(outMetadata.getFieldPosition(outputField), 
				inMetadata.getFieldPosition(inputField));
	}
	
	/**
	 * Registers a mapping of a constant onto an output field.
	 * 
	 * @param value value of the constant.
	 * @param outputField
	 */
	private void addConstantMapping(Object value, String outputField) {
		constantMapping.add(new ConstantMappingItem(value, outMetadata.getFieldPosition(outputField)));
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
		
		public AggregationGroup(DataRecord firstInput) throws AggregationException {
			storeKeyFields(firstInput);
			int count = functionMapping.size();
			functions = new AggregateFunction[count];
			for (int i = 0; i < count; i++) {
				FunctionMappingItem mapping = functionMapping.get(i);
				AggregateFunction function = createFunctionInstance(mapping.getFunction());
				function.setInputFieldIndex(mapping.getInputFieldIndex());
				function.setInputFieldMetadata(inMetadata.getField(mapping.getInputFieldIndex()));
				function.setOutputFieldIndex(mapping.getOutputFieldIndex());
				function.setOutputFieldMetadata(outMetadata.getField(mapping.getOutputFieldIndex()));
				
				functions[i] = function;
			}
		}
		
		public void update(DataRecord inputRecord) throws Exception {
			for (AggregateFunction function : functions) {
				try {
					function.update(inputRecord);
				} catch (Exception e) {
					// report failed function and field
					throw new RuntimeException("Exception in aggregate function '" + function.getName() +
							"' on field '" + function.getInputFieldMetadata().getName() + "'", e);
				}
			}
		}
		
		public void storeResult(DataRecord outRecord) {
			applyFieldMapping(outRecord);
			applyConstantMapping(outRecord);
			for (AggregateFunction function : functions) {
				try {
					function.storeResult(outRecord.getField(function.getOutputFieldIndex()));
				} catch (Exception e) {
					// report failed function and fields
					throw new RuntimeException("Failed to store result of aggregate function '" + function.getName() +
							"' of field '" + function.getInputFieldMetadata().getName() +
							"' into field '" + function.getOutputFieldMetadata().getName() + "'", e);
				}
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
		 * Applies the constant mapping.
		 * 
		 * @param outputRecord
		 */
		private void applyConstantMapping(DataRecord outputRecord) {
			for (ConstantMappingItem item : constantMapping) {
				outputRecord.getField(item.getOutputField()).setValue(item.getValue());
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
	 * Represents one mapping of a constant onto an output field.
	 * 
	 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
	 *         (c) Javlin Consulting (www.javlinconsulting.cz)
	 *
	 * @created 30.4.2007
	 */
	private static class ConstantMappingItem {
		private int outputField;
		private Object value;
		
		/**
		 * 
		 * Allocates a new <tt>ConstantMappingItem</tt> object.
		 *
		 * @param value
		 * @param outputField
		 */
		public ConstantMappingItem(Object value, int outputField) {
			this.outputField = outputField;
			this.value = value;
		}

		/**
		 * 
		 * @return index of the output field used to store the constant.
		 */
		public int getOutputField() {
			return outputField;
		}

		/**
		 * 
		 * @return value of the constant.
		 */
		public Object getValue() {
			return value;
		}
	}
	
	/**
	 * Iterator over the results of unsorted aggregation.
	 * 
	 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
	 *         (c) Javlin Consulting (www.javlinconsulting.cz)
	 */
	private class UnsortedResultsIterator implements Iterator<DataRecord> {
		Iterator<HashKey> keyIterator;
		DataRecord outRecord;
		
		public UnsortedResultsIterator(DataRecord outRecord) {
			this.outRecord = outRecord;
			keyIterator = unsortedGroups.keySet().iterator();
		}
		@Override
		public boolean hasNext() {
			return keyIterator.hasNext();
		}

		@Override
		public DataRecord next() {
			unsortedGroups.get(keyIterator.next()).storeResult(outRecord);
			return outRecord;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Removal of aggregation results is not supported");
		}
	}
}
