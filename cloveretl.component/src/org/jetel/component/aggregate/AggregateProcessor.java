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
	// registry of available aggregate functions
	private FunctionRegistry functionRegistry = new FunctionRegistry();
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
	public AggregateProcessor(String[] mapping, boolean oldMapping, RecordKey recordKey, boolean sorted, 
			DataRecordMetadata inMetadata, DataRecordMetadata outMetadata, String charset) 
	throws AggregationException {
		this.recordKey = recordKey;
		
		this.sorted = sorted;
		if (!sorted) {
			unsortedGroups = new HashMap<String, AggregationGroup>();
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
	private void processMapping(String[] mapping) throws AggregationException {
		AggregateMappingParser parser = new AggregateMappingParser(mapping, 
				recordKey, functionRegistry,
				inMetadata, outMetadata);
		List<AggregateMappingParser.FunctionMapping> functionMappings = parser.getFunctionMapping();
		List<AggregateMappingParser.FieldMapping> fieldMappings = parser.getFieldMapping();
		
		for (AggregateMappingParser.FieldMapping fieldMapping : fieldMappings) {
			addFieldMapping(fieldMapping.getInputField(), fieldMapping.getOutputField());
		}
		for (AggregateMappingParser.FunctionMapping functionMapping : functionMappings) {
			addFunctionMapping(functionMapping.getFunctionName(), 
					functionMapping.getInputField(),
					functionMapping.getOutputField());
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

		// TODO potrebne?
		if (!inputField.equals("")) {
			f.setInputFieldMetadata(inMetadata.getField(inputField));
		}
		
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
